# -*- coding: utf-8 -*-
"""
output_value_file.py

manage a single value file, while it is being written
"""
from datetime import datetime
import hashlib
import logging
import os
import os.path

import psycopg2

from tools.data_definitions import compute_value_file_path, \
        value_file_template

_sync_strategy = os.environ.get("NIMBUSIO_SYNC_STRATEGY", "NONE")

def _get_next_value_file_id(connection):
    (next_value_file_id, ) = connection.fetch_one_row(
        "select nextval('nimbusio_node.value_file_id_seq');"
    )
    connection.commit()
    return next_value_file_id

def _open_value_file(value_file_path):
    value_file_dir = os.path.dirname(value_file_path)
    if not os.path.exists(value_file_dir):
        os.makedirs(value_file_dir)
    flags = os.O_WRONLY | os.O_CREAT
    if _sync_strategy == "O_SYNC":
        flags |= os.O_SYNC
    return os.open(value_file_path, flags)

def _insert_value_file_row(connection, value_file_row):
    """
    Insert one value_file entry
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into nimbusio_node.value_file (
            id,
            creation_time,
            close_time,
            size,
            hash,
            sequence_count,
            min_segment_id,
            max_segment_id,
            distinct_collection_count,
            collection_ids,
            garbage_size_estimate,
            fragmentation_estimate,
            last_cleanup_check_time,
            last_integrity_check_time
        ) values (
            %(id)s,
            %(creation_time)s::timestamp,
            %(close_time)s::timestamp,
            %(size)s,
            %(hash)s,
            %(sequence_count)s,
            %(min_segment_id)s,
            %(max_segment_id)s,
            %(distinct_collection_count)s,
            %(collection_ids)s,
            %(garbage_size_estimate)s,
            %(fragmentation_estimate)s,
            %(last_cleanup_check_time)s::timestamp,
            %(last_integrity_check_time)s::timestamp
        )
        returning id
    """, value_file_row._asdict())
    connection.commit()

class OutputValueFile(object):
    def __init__(self, connection, repository_path):
        self._value_file_id = _get_next_value_file_id(connection)
        self._log = logging.getLogger("VF%08d" % (self._value_file_id, ))
        self._connection = connection
        self._value_file_path = compute_value_file_path(
             repository_path, self._value_file_id
        )
        self._log.info("opening %s" % (self._value_file_path, )) 
        self._value_file_fd = _open_value_file(self._value_file_path)
        self._creation_time = datetime.now()
        self._size = 0L
        self._md5 = hashlib.md5()
        self._sequence_count = 0
        self._min_segment_id = None
        self._max_segment_id = None
        self._collection_ids = set()

    @property
    def value_file_id(self):
        return self._value_file_id

    @property
    def size(self):
        return self._size

    def write_data_for_one_sequence(self, collection_id, segment_id, data):
        """
        write the data for one sequence
        """
        os.write(self._value_file_fd, data)
        self._size += len(data)
        self._md5.update(data)
        self._sequence_count += 1
        if self._min_segment_id is None:
            self._min_segment_id = segment_id
        else:
            self._min_segment_id = min(self._min_segment_id, segment_id)
        if self._max_segment_id is None:
            self._max_segment_id = segment_id
        else:
            self._max_segment_id = max(self._max_segment_id, segment_id)
        self._collection_ids.add(collection_id)

    def close(self):
        """close the file and make it visible in the database"""
        os.close(self._value_file_fd)

        if self._sequence_count == 0:
            self._log.info("removing empty file %s" % (self._value_file_path,)) 
            try:
                os.unlink(self._value_file_path)
            except Exception:
                pass
            return

        self._log.info("closing %s size=%s sequence_count=%s" % (
            self._value_file_path, self._size, self._sequence_count
        )) 
        value_file_row = value_file_template(
            id=self._value_file_id,
            creation_time=self._creation_time,
            close_time=datetime.now(),
            size=self._size,
            hash=psycopg2.Binary(self._md5.digest()),
            sequence_count=self._sequence_count,
            min_segment_id=self._min_segment_id,
            max_segment_id=self._max_segment_id,
            distinct_collection_count=len(self._collection_ids),
            collection_ids=sorted(list(self._collection_ids)),
            garbage_size_estimate=0,
            fragmentation_estimate=0,
            last_cleanup_check_time=None,
            last_integrity_check_time=None
        )
        _insert_value_file_row(self._connection, value_file_row)

