# -*- coding: utf-8 -*-
"""
output_value_file.py

manage a single value file, while it is being written
"""
import hashlib
import logging
import os
import os.path

import psycopg2

from tools.data_definitions import compute_value_file_path, \
        value_file_template, \
        create_timestamp

def _insert_value_file_default_row(connection, space_id):
    # Ticket #1646: insert a row of defaults right at open
    value_file_id = connection.execute_and_return_id("""
        insert into nimbusio_node.value_file (space_id) values (%s)
        returning id
    """, [space_id, ])
    connection.commit()

    return value_file_id

def _open_value_file(value_file_path):
    value_file_dir = os.path.dirname(value_file_path)
    if not os.path.exists(value_file_dir):
        os.makedirs(value_file_dir)
    flags = os.O_WRONLY | os.O_CREAT
    return os.open(value_file_path, flags)

def _update_value_file_row(connection, value_file_row):
    """
    Insert one value_file entry
    """
    connection.execute("""
        update nimbusio_node.value_file set
            creation_time=%(creation_time)s::timestamp,
            close_time=%(close_time)s::timestamp,
            size=%(size)s,
            hash=%(hash)s,
            segment_sequence_count=%(segment_sequence_count)s,
            min_segment_id=%(min_segment_id)s,
            max_segment_id=%(max_segment_id)s,
            distinct_collection_count=%(distinct_collection_count)s,
            collection_ids=%(collection_ids)s
        where id = %(id)s
    """, value_file_row._asdict())
    connection.commit()

def mark_value_files_as_closed(connection):
    """
    mark as closed any files that were left marked open:
    set close_time in any rows where it is null
    """
    connection.execute(
        """
        update nimbusio_node.value_file set close_time=current_timestamp
        where close_time is null
        """, [])
    connection.commit()

class OutputValueFile(object):
    def __init__(self, connection, repository_path):
        # XXX temporary expedient until we implement file spaces
        self._space_id = -1
        self._value_file_id =  _insert_value_file_default_row(connection,
                                                             self._space_id)
        self._log = logging.getLogger("VF%08d" % (self._value_file_id, ))
        self._connection = connection
        self._value_file_path = compute_value_file_path(
             repository_path, self._value_file_id
        )
        self._log.info("opening %s" % (self._value_file_path, )) 
        self._value_file_fd = _open_value_file(self._value_file_path)
        self._creation_time = create_timestamp()
        self._size = 0L
        self._md5 = hashlib.md5()
        self._segment_sequence_count = 0
        self._min_segment_id = None
        self._max_segment_id = None
        self._collection_ids = set()
        self._synced = True # treat as synced until we write

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
        self._synced = False

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

    def sync(self):
        """
        sync this file to disk (if neccessary)
        """
        if not self._synced:
            os.fsync(self._value_file_fd)
            self._synced = True

    @property
    def is_synced(self):
        return self._synced

    def close(self):
        """close the file and make it visible in the database"""
        self.sync()
        os.close(self._value_file_fd)

        if self._segment_sequence_count == 0:
            self._log.info("removing empty file %s" % (self._value_file_path,)) 
            try:
                os.unlink(self._value_file_path)
            except Exception:
                pass
            return

        self._log.info("closing %s size=%s segment_sequence_count=%s" % (
            self._value_file_path, self._size, self._segment_sequence_count
        )) 
        value_file_row = value_file_template(
            id=self._value_file_id,
            creation_time=self._creation_time,
            close_time=create_timestamp(),
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
        _update_value_file_row(self._connection, value_file_row)

