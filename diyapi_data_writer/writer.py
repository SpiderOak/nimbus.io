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
        self._active_segments = dict()

        # open a new value file at startup
        self._output_value_file = OutputValueFile(connection, repository_path)

    def close(self):
        self._output_value_file.close()

    def start_new_key(self, avatar_id, key, timestamp, segment_num):
        segment_key = (avatar_id, key, timestamp, segment_num, )
        if segment_key in self._active_segments:
            raise ValueError("duplicate segment %s" % (segment_key, ))

        self._active_segments[segments_key] = {
            "key-id"            : _get_next_key_id(),
            "segment-md5"       : hashlib.md5(),
            "segment-size"      : 0,
            "segment_adler32"   : zlib.adler32("")
        }

    def store_key_sequence(
        self, avatar_id, key, timestamp, segment_num, sequence_num, data
    ):
        segment_key = (avatar_id, key, timestamp, segment_num, )
        segment_entry = self._active_writes[active_key]

        sequence_md5 = hashlib.md5()
        sequence_md5.update(data)
        segment_entry["key-md5"].update(data)

        key_sequence_row = key_sequence_template(
            avatar_id=avatar_id,
            key_id=segment_entry["key-id"],
            value_file_id=self._value_file.value_file_id,
            sequence_num=sequence_num,
            value_file_offset=self._value_file.size,
            size=len(data),
            hash=psycopg2.Binary(sequence_md5.digest()),
            adler32=zlib.adler32(data),
        )

        segment_entry["segment-md5"].update(data)
        segment_entry["segment-size"] += len(data)
        segment_entry["segment-adler32"] += zlib.adler32(
            data, segment_entry["segment-adler32"] 
        )

        self._value_file.write_data_for_one_sequence(
            avatar_id, segment_entry["key-id"], data
        )

        _insert_key_sequence_row(self._database_connection, key_sequence_row)

    def finish_new_key(self, avatar_id, key, timestamp, segment_num): 

        segment_key = (avatar_id, key, timestamp, segment_num, )
        segment_entry = self._active_writes.pop(segment_key)

        key_row = key_row_template(
            name=key,
            id=segment_entry["key-id"],
            avatar_id=avatar_id,
            timestamp=datetime.fromtimestamp(timestamp),
            size=segment_entry["segment-size"],
            adler32=segment_entry["segment-adler32"],
            hash=psycopg2.Binary(segment_entry["segment-md5"].digest()),
            user_id=0,
            group_id=0,
            permissions=0,
            tombstone=False,
            segment_num=segment_num,
            handoff_node_id=None
        )
        _insert_key_row(self._database_connection, key_row)
    
