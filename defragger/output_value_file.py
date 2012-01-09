# -*- coding: utf-8 -*-
"""
A value file for defragged output
"""
from datetime import datetime
import hashlib
import logging
import os

import psycopg2

from tools.data_definitions import compute_value_file_path, \
        value_file_template

def _get_next_value_file_id(connection):
    """
    To avoid blocking of concurrent transactions that obtain numbers from the 
    same sequence, a nextval operation is never rolled back; that is, once a 
    value has been fetched it is considered used, even if the transaction that 
    did the nextval later aborts
    """
    (next_value_file_id, ) = connection.fetch_one_row(
        "select nextval('nimbusio_node.value_file_id_seq');"
    )
    return next_value_file_id

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
            segment_sequence_count,
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
            %(segment_sequence_count)s,
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

class OutputValueFile(object):
    """
    A value file for defragged output
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogger("OutputValueFile")
        self._connection = connection
        self._value_file_id = _get_next_value_file_id(connection)
        self._value_file_path = compute_value_file_path(
            repository_path, self._value_file_id
        )
        self._log.info("opening {0}".format(self._value_file_path)) 
        value_file_dir = os.path.dirname(self._value_file_path)
        if not os.path.exists(value_file_dir):
            os.makedirs(value_file_dir)
        flags = os.O_WRONLY | os.O_CREAT
        self._value_file_fd = os.open(self._value_file_path, flags)
        self._creation_time = datetime.now()
        self._size = 0
        self._md5 = hashlib.md5()
        self._segment_sequence_count = 0
        self._min_segment_id = None
        self._max_segment_id = None
        self._collection_ids = set()

    def write_data_for_one_sequence(self, collection_id, segment_id, data):
        """
        write the data for one sequence
        """
        os.write(self._value_file_fd, data)
        self._size += len(data)
        self._md5.update(data)
        self._segment_sequence_count += 1
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
        self._log.info("closing %s size=%s segment_sequence_count=%s" % (
            self._value_file_path, self._size, self._segment_sequence_count
        )) 

        os.close(self._value_file_fd)
        #TODO: fsync

        value_file_row = value_file_template(
            id=self._value_file_id,
            creation_time=self._creation_time,
            close_time=datetime.now(),
            size=self._size,
            hash=psycopg2.Binary(self._md5.digest()),
            segment_sequence_count=self._segment_sequence_count,
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

