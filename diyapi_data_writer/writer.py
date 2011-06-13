# -*- coding: utf-8 -*-
"""
writer.py

Manage writing key values to disk
"""
import hashlib
import logging

import zlib

import psycopg2

from diyapi_data_writer.output_value_file import OutputValueFile

key_row_template = namedtuple(
    "KeyRow", [
        "name",
        "id",
        "avatar_id",
        "timestamp",
        "size",
        "adler32",
        "hash",
        "user_id",
        "group_id",
        "permissions",
        "tombstone",
        "segment_num",
        "handoff_node_id",
    ]
)

key_sequence_template = namedtuple(
    "KeySequence", [
        "avatar_id",
        "key_id",
        "value_file_id",
        "sequence_num",
        "value_file_offset",
        "size",
        "hash",
        "adler32",
    ]
)

def _get_next_key_id(connection):
    (next_key_id, ) = connection.fetch_one_row(
        "select nextval('diy.key_id_seq');"
    )
    connection.commit()
    return next_key_id

def _insert_key_row(connection, key_row):
    """
    Insert one key entry, returning the row id
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into diy.key (
            name,
            id,
            avatar_id,
            timestamp,
            size,
            adler32,
            hash,
            user_id,
            group_id,
            permissions,
            tombstone,
            segment_num,
            handoff_node_id
        ) values (
            %(name)s,
            %(id)s,
            %(avatar_id)s,
            %(timestamp)s::timestamp,
            %(size)s,
            %(adler32)s,
            %(hash)s,
            %(user_id)s,
            %(group_id)s,
            %(permissions)s,
            %(tombstone)s,
            %(segment_num)s,
            %(handoff_node_id)s
        )
    """, key_row._asdict())
    cursor.close()
    connection.commit()

def _insert_key_sequence_row(connection, key_sequence_row):
    """
    Insert one key_sequence entry
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into diy.key_sequence (
        "avatar_id",
        "key_id",
        "value_file_id",
        "sequence_num",
        "value_file_offset",
        "size",
        "hash",
        "adler32",
            name,
            avatar_id,
            timestamp,
            size,
            adler32,
            hash,
            user_id,
            group_id,
            permissions,
            tombstone,
            segment_num,
            handoff_node_id
        ) values (
            %(name)s,
            %(avatar_id)s,
            %(timestamp)s::timestamp,
            %(size)s,
            %(adler32)s,
            %(hash)s,
            %(user_id)s,
            %(group_id)s,
            %(permissions)s,
            %(tombstone)s,
            %(segment_num)s,
            %(handoff_node_id)s
        )
    """, key_row._asdict())
    cursor.close()
    connection.commit()

class Writer(object):
    """
    Manage writing key values to disk
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogge("Writer")
        self._connection = connection
        self._active_writes = dict()

        # open a new value file at startup
        self._output_value_file = OutputValueFile(connection, repository_path)

    def close(self):
        self._output_value_file.close()

    def start_new_key(self, avatar_id, key, timestamp, segment_num):
        active_key = (avatar_id, key, timestamp, segment_num, )
        if active_key in self._active_writes:
            raise ValueError("duplicate new key %s" % (active_key, ))

        self._active_writes[active_key] = {
            "key-id" : _get_next_key_id(),
            "key-md5": hashlib.md5()
        }

    def store_key_sequence(
        self, avatar_id, key, timestamp, segment_num, sequence_num, data
    ):
        active_key = (avatar_id, key, timestamp, segment_num, )
        active_entry = self._active_writes[active_key]

        sequence_md5 = hashlib.md5()
        sequence_md5.update(data)
        active_entry["key-md5"].update(data)

        key_sequence_row = key_sequence_template(
            avatar_id=avatar_id,
            key_id=active_entry["key-id"],
            value_file_id=self._value_file.value_file_id,
            sequence_num=sequence_num,
            value_file_offset=self._value_file.size,
            size=len(data),
            hash=psycopg2.Binary(sequence_md5.digest()),
            adler32=zlib.adler32(data),
        )

        self._value_file.write_data_for_one_sequence(
            avatar_id, active_entry["key-id"], data
        )

        _insert_key_sequence_row(self._database_connection, key_sequence_row)

    def finish_new_key(
        self, avatar_id, key, timestamp, segment_num, file_adler32
    ): 

        active_key = (avatar_id, key, timestamp, segment_num, )
        active_entry = self._active_writes[active_key]

        key_row = key_row_template(
            name=key,
            id=active_entry["key-id"],
            avatar_id=avatar_id,
            timestamp=datetime.fromtimestamp(timestamp),
            size=active_entry["size"],
            adler32=active_entry["adler32"],
            hash=psycopg2.Binary(active_entry["key-md5"].digest()),
            user_id=0,
            group_id=0,
            permissions=0,
            tombstone=False,
            segment_num=segment_num,
            handoff_node_id=None
        )
        _insert_key_row(self._database_connection, key_row)
    
