# -*- coding: utf-8 -*-
"""
output_value_file.py

manage a single value file, while it is being written
"""
from collections import namedtuple
from datetime import datetime
import hashlib
import logging
import os
import os.path

import psycopg2

value_file_template = namedtuple("ValueFile", [
    "id",
    "creation_time",
    "close_time",
    "size",
    "hash",
    "sequence_count",
    "min_key_id",
    "max_key_id",
    "distinct_avatar_count",
    "avatar_ids",
    "garbage_size_estimate",
    "fragmentation_estimate",
    "last_cleanup_check_time",
    "last_integrity_check_time"]
)

def _get_next_value_file_id(connection):
    (next_value_file_id, ) = connection.fetch_one_row(
        "select nextval('diy.value_file_id_seq');"
    )
    connection.commit()
    return next_value_file_id

def _compute_value_file_path(value_file_id, repository_path):
    return os.path.join(
        repository_path, 
        "%03d" % (value_file_id % 1000), 
        "%08d" % value_file_id
    )

def _open_value_file(value_file_path):
    value_file_dir = os.path.dirname(value_file_path)
    if not os.path.exists(value_file_dir):
        os.makedirs(value_file_dir)
    return os.open(value_file_path, os.O_WRONLY | os.O_CREAT | os.O_SYNC)

def _insert_value_file_row(connection, value_file_row):
    """
    Insert one value_file entry
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into diy.value_file (
            id,
            creation_time,
            close_time,
            size,
            hash,
            sequence_count,
            min_key_id,
            max_key_id,
            distinct_avatar_count,
            avatar_ids,
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
            %(min_key_id)s,
            %(max_key_id)s,
            %(distinct_avatar_count)s,
            %(avatar_ids)s,
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
        self._value_file_path = _compute_value_file_path(
            self._value_file_id , repository_path
        )
        self._log.info("opening %s" % (self._value_file_path, )) 
        self._value_file_fd = _open_value_file(self._value_file_path)
        self._creation_time = datetime.now()
        self._size = 0L
        self._md5 = hashlib.md5()
        self._sequence_count = 0
        self._min_key_id = None
        self._max_key_id = None
        self._avatar_ids = set()

    @property
    def value_file_id(self):
        return self._value_file_id

    @property
    def size(self):
        return self._size

    def write_data_for_one_sequence(self, avatar_id, key_id, data):
        """
        write the data for one sequence, acumulating meta information
        """
        os.write(self._value_file_fd, data)
        self._size += len(data)
        self._md5.update(data)
        self._sequence_count += 1
        if self._min_key_id is None:
            self._min_key_id = key_id
        else:
            self._min_key_id = min(self._min_key_id, key_id)
        if self._max_key_id is None:
            self._max_key_id = key_id
        else:
            self._max_key_id = max(self._max_key_id, key_id)
        self._avatar_ids.add(avatar_id)

    def close(self):
        """close the file and make it visible in the database"""
        self._log.info("closing %s size=%s sequence_count=%s" % (
            self._value_file_path, self._size, self._sequence_count
        )) 
        os.close(self._value_file_fd)
        value_file_row = value_file_template(
            id=self._value_file_id,
            creation_time=self._creation_time,
            close_time=datetime.now(),
            size=self._size,
            hash=psycopg2.Binary(self._md5.digest()),
            sequence_count=self._sequence_count,
            min_key_id=self._min_key_id,
            max_key_id=self._max_key_id,
            distinct_avatar_count=len(self._avatar_ids),
            avatar_ids=sorted(list(self._avatar_ids)),
            garbage_size_estimate=0,
            fragmentation_estimate=0,
            last_cleanup_check_time=None,
            last_integrity_check_time=None
        )
        _insert_value_file_row(self._connection, value_file_row)

