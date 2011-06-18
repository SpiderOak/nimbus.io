# -*- coding: utf-8 -*-
"""
writer.py

Manage writing segment values to disk
"""
from collections import namedtuple
from datetime import datetime
import hashlib
import logging
import zlib

import psycopg2

from diyapi_tools.standard_logging import format_timestamp
from diyapi_data_writer.output_value_file import OutputValueFile

segment_row_template = namedtuple(
    "SegmentRow", [
        "id",
        "avatar_id",
        "key",
        "timestamp",
        "segment_num",
        "file_size",
        "file_adler32",
        "file_hash",
        "file_user_id",
        "file_group_id",
        "file_permissions",
        "file_tombstone",
        "handoff_node_id",
    ]
)

segment_sequence_template = namedtuple(
    "egmentSequence", [
        "avatar_id",
        "segment_id",
        "value_file_id",
        "sequence_num",
        "value_file_offset",
        "size",
        "hash",
        "adler32",
    ]
)

def _get_next_segment_id(connection):
    (next_segment_id, ) = connection.fetch_one_row(
        "select nextval('diy.segment_id_seq');"
    )
    connection.commit()
    return next_segment_id

def _insert_segment_row(connection, segment_row):
    """
    Insert one segment entry, returning the row id
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into diy.segment (
            id,
            avatar_id,
            key,
            timestamp,
            segment_num,
            file_size,
            file_adler32,
            file_hash,
            file_user_id,
            file_group_id,
            file_permissions,
            file_tombstone,
            handoff_node_id
        ) values (
            %(id)s,
            %(avatar_id)s,
            %(key)s,
            %(timestamp)s::timestamp,
            %(segment_num)s,
            %(file_size)s,
            %(file_adler32)s,
            %(file_hash)s,
            %(file_user_id)s,
            %(file_group_id)s,
            %(file_permissions)s,
            %(file_tombstone)s,
            %(handoff_node_id)s
        )
    """, segment_row._asdict())
    cursor.close()
    connection.commit()

def _insert_segment_sequence_row(connection, segment_sequence_row):
    """
    Insert one segment_sequence entry
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into diy.segment_sequence (
            "avatar_id",
            "segment_id",
            "value_file_id",
            "sequence_num",
            "value_file_offset",
            "size",
            "hash",
            "adler32"
        ) values (
            %(avatar_id)s,
            %(segment_id)s,
            %(value_file_id)s,
            %(sequence_num)s,
            %(value_file_offset)s,
            %(size)s,
            %(hash)s,
            %(adler32)s
        )
    """, segment_sequence_row._asdict())
    cursor.close()
    connection.commit()

class Writer(object):
    """
    Manage writing segment values to disk
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogger("Writer")
        self._connection = connection
        self._active_segments = dict()

        # open a new value file at startup
        self._value_file = OutputValueFile(connection, repository_path)

    def close(self):
        self._value_file.close()

    def start_new_segment(self, avatar_id, key, timestamp, segment_num):
        """
        Initiate storing a segment of data for a file
        """
        segment_key = (avatar_id, key, timestamp, segment_num, )
        self._log.info("start_new_segment %s %s %s %s" % (
            avatar_id, key, format_timestamp(timestamp), segment_num, 
        ))
        if segment_key in self._active_segments:
            raise ValueError("duplicate segment %s" % (segment_key, ))

        self._active_segments[segment_key] = {
            "segment-id"            : _get_next_segment_id(self._connection),
        }

    def store_sequence(
        self, avatar_id, key, timestamp, segment_num, sequence_num, data
    ):
        """
        store one piece (sequence) of segment data
        """
        segment_key = (avatar_id, key, timestamp, segment_num, )
        self._log.info("store_sequence %s %s %s %s: %s (%s)" % (
            avatar_id, 
            key, 
            format_timestamp(timestamp), 
            segment_num, 
            sequence_num,
            len(data)
        ))
        segment_entry = self._active_segments[segment_key]

        sequence_md5 = hashlib.md5()
        sequence_md5.update(data)

        segment_sequence_row = segment_sequence_template(
            avatar_id=avatar_id,
            segment_id=segment_entry["segment-id"],
            value_file_id=self._value_file.value_file_id,
            sequence_num=sequence_num,
            value_file_offset=self._value_file.size,
            size=len(data),
            hash=psycopg2.Binary(sequence_md5.digest()),
            adler32=zlib.adler32(data),
        )

        self._value_file.write_data_for_one_sequence(
            avatar_id, segment_entry["segment-id"], data
        )

        _insert_segment_sequence_row(self._connection, segment_sequence_row)

    def finish_new_segment(
        self, 
        avatar_id, 
        key, 
        timestamp, 
        segment_num,
        file_size,
        file_adler32,
        file_hash,
        file_user_id,
        file_group_id,
        file_permissions,
        file_tombstone
    ): 
        """
        finalize storing one segment of data for a file
        """

        segment_key = (avatar_id, key, timestamp, segment_num, )
        self._log.info("finish_new_segment %s %s %s %s" % (
            avatar_id, key, format_timestamp(timestamp), segment_num, 
        ))
        segment_entry = self._active_segments.pop(segment_key)

        segment_row = segment_row_template(
            id=segment_entry["segment-id"],
            avatar_id=avatar_id,
            key=key,
            timestamp=datetime.fromtimestamp(timestamp),
            segment_num=segment_num,
            file_size=file_size,
            file_adler32=file_adler32,
            file_hash=psycopg2.Binary(file_hash),
            file_user_id=file_user_id,
            file_group_id=file_group_id,
            file_permissions=file_permissions,
            file_tombstone=file_tombstone,
            handoff_node_id=None
        )
        _insert_segment_row(self._connection, segment_row)
    
