# -*- coding: utf-8 -*-
"""
writer.py

Manage writing segment values to disk
"""
import hashlib
import logging
import os
import zlib

import psycopg2

from diyapi_tools.data_definitions import segment_row_template, \
        segment_sequence_template, \
        parse_timestamp_repr, \
        meta_row_template
from diyapi_data_writer.output_value_file import OutputValueFile

_max_value_file_size = os.environ.get(
    "NIMBUSIO_MAX_VALUE_FILE_SIZE", str(1024 * 1024 * 1024)
)

def _get_next_segment_id(connection):
    (next_segment_id, ) = connection.fetch_one_row(
        "select nextval('nimbusio_node.segment_id_seq');"
    )
    connection.commit()
    return next_segment_id

def _insert_segment_row_with_meta(connection, segment_row, meta_rows):
    """
    Insert one segment entry, with pre-assigned row id, along with
    associated meta rows
    """
    segment_row_dict = segment_row._asdict()
    connection.execute("""
        insert into nimbusio_node.segment (
            id,
            collection_id,
            key,
            timestamp,
            segment_num,
            file_size,
            file_adler32,
            file_hash,
            file_tombstone,
            handoff_node_id
        ) values (
            %(id)s,
            %(collection_id)s,
            %(key)s,
            %(timestamp)s::timestamp,
            %(segment_num)s,
            %(file_size)s,
            %(file_adler32)s,
            %(file_hash)s,
            %(file_tombstone)s,
            %(handoff_node_id)s
        )
    """, segment_row_dict)
    for meta_row in meta_rows:
        meta_row_dict = meta_row._asdict()
        connection.execute("""
            insert into nimbusio_node.meta (
                collection_id,
                key,
                meta_key,
                meta_value,
                timestamp
            ) values (
                %(collection_id)s,
                %(key)s,
                %(meta_key)s,
                %(meta_value)s,
                %(timestamp)s::timestamp
            )
        """, meta_row_dict)            

    connection.commit()

def _insert_segment_tombstone_row(connection, segment_row):
    """
    Insert one segment entry, with default row id
    """
    segment_row_dict = segment_row._asdict()
    connection.execute("""
        insert into nimbusio_node.segment (
            collection_id,
            key,
            timestamp,
            segment_num,
            file_size,
            file_adler32,
            file_hash,
            file_tombstone,
            handoff_node_id
        ) values (
            %(collection_id)s,
            %(key)s,
            %(timestamp)s::timestamp,
            %(segment_num)s,
            %(file_size)s,
            %(file_adler32)s,
            %(file_hash)s,
            %(file_tombstone)s,
            %(handoff_node_id)s
        )
    """, segment_row_dict)
    connection.commit()

def _insert_segment_sequence_row(connection, segment_sequence_row):
    """
    Insert one segment_sequence entry
    """
    connection.execute("""
        insert into nimbusio_node.segment_sequence (
            "collection_id",
            "segment_id",
            "value_file_id",
            "sequence_num",
            "value_file_offset",
            "size",
            "hash",
            "adler32"
        ) values (
            %(collection_id)s,
            %(segment_id)s,
            %(value_file_id)s,
            %(sequence_num)s,
            %(value_file_offset)s,
            %(size)s,
            %(hash)s,
            %(adler32)s
        )
    """, segment_sequence_row._asdict())
    connection.commit()

def _get_segment_id(connection, collection_id, key, timestamp, segment_num): 
    result = connection.fetch_one_row(""" 
        select id from nimbusio_node.segment
        where collection_id = %s and key = %s and timestamp = %s::timestamp
        and segment_num = %s""",
        [collection_id, key, timestamp, segment_num, ]
    )
    if result is None:
        return None
    (segment_id, ) = result
    return segment_id

def _purge_segment_rows(connection, segment_id):
    connection.execute("""
        delete from nimbusio_node.segment_sequence where segment_id = %s;
        delete from nimbusio_node.segment where id = %s;
        """, [segment_id, segment_id]
    )
    connection.commit()

class Writer(object):
    """
    Manage writing segment values to disk
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogger("Writer")
        self._connection = connection
        self._repository_path = repository_path
        self._active_segments = dict()

        # open a new value file at startup
        self._value_file = OutputValueFile(
            self._connection, self._repository_path
        )

    def close(self):
        self._value_file.close()

    def start_new_segment(
        self, 
        collection_id, 
        key, 
        timestamp_repr, 
        segment_num
    ):
        """
        Initiate storing a segment of data for a file
        """
        segment_key = (collection_id, key, timestamp_repr, segment_num, )
        self._log.info("start_new_segment %s %s %s %s" % (
            collection_id, key, timestamp_repr, segment_num, 
        ))
        if segment_key in self._active_segments:
            raise ValueError("duplicate segment %s" % (segment_key, ))

        self._active_segments[segment_key] = {
            "segment-id"            : _get_next_segment_id(self._connection),
        }

    def store_sequence(
        self, 
        collection_id, 
        key, 
        timestamp_repr, 
        segment_num, 
        sequence_num, 
        data
    ):
        """
        store one piece (sequence) of segment data
        """
        segment_key = (collection_id, key, timestamp_repr, segment_num, )
        self._log.info("store_sequence %s %s %s %s: %s (%s)" % (
            collection_id, 
            key, 
            timestamp_repr, 
            segment_num, 
            sequence_num,
            len(data)
        ))
        segment_entry = self._active_segments[segment_key]

        # if this write would put us over the max size,
        # start a new output value file
        if self._value_file.size + len(data) > _max_value_file_size:
            self._value_file.close()
            self._value_file = OutputValueFile(
                self._connection, self._repository_path
            )

        sequence_md5 = hashlib.md5()
        sequence_md5.update(data)

        segment_sequence_row = segment_sequence_template(
            collection_id=collection_id,
            segment_id=segment_entry["segment-id"],
            value_file_id=self._value_file.value_file_id,
            sequence_num=sequence_num,
            value_file_offset=self._value_file.size,
            size=len(data),
            hash=psycopg2.Binary(sequence_md5.digest()),
            adler32=zlib.adler32(data),
        )

        self._value_file.write_data_for_one_sequence(
            collection_id, segment_entry["segment-id"], data
        )

        _insert_segment_sequence_row(self._connection, segment_sequence_row)

    def finish_new_segment(
        self, 
        collection_id, 
        key, 
        timestamp_repr, 
        meta_dict,
        segment_num,
        conjoined_id,
        conjoined_num,
        conjoined_complete,
        file_size,
        file_adler32,
        file_hash,
        file_tombstone,
        handoff_node_id
    ): 
        """
        finalize storing one segment of data for a file
        """
        segment_key = (collection_id, key, timestamp_repr, segment_num, )
        self._log.info("finish_new_segment %s %s %s %s" % (
            collection_id, key, timestamp_repr, segment_num, 
        ))
        segment_entry = self._active_segments.pop(segment_key)

        timestamp = parse_timestamp_repr(timestamp_repr)

        segment_row = segment_row_template(
            id=segment_entry["segment-id"],
            collection_id=collection_id,
            key=key,
            timestamp=timestamp,
            segment_num=segment_num,
            conjoined_id=conjoined_id,
            conjoined_num=conjoined_num,
            conjoined_complete=conjoined_complete,
            file_size=file_size,
            file_adler32=file_adler32,
            file_hash=psycopg2.Binary(file_hash),
            file_tombstone=file_tombstone,
            handoff_node_id=handoff_node_id
        )
        meta_rows = list()
        for meta_key, meta_value in meta_dict.items():
            meta_row = meta_row_template(
                collection_id=collection_id,
                key=key,
                meta_key=meta_key,
                meta_value=meta_value,
                timestamp=timestamp
            )
            meta_rows.append(meta_row)

        _insert_segment_row_with_meta(self._connection, segment_row, meta_rows)
    
    def purge_segment(self, collection_id, key, timestamp, segment_num):
        """
        remove all database rows referring to a segment
        """
        segment_id = _get_segment_id(
            self._connection, collection_id, key, timestamp, segment_num
        )
        if segment_id is not None:
            _purge_segment_rows(self._connection, segment_id)
        
    def set_tombstone(self, collection_id, key, timestamp, segment_num):
        """
        mark a key as deleted
        """
        segment_row = segment_row_template(
            id=None,
            collection_id=collection_id,
            key=key,
            timestamp=timestamp,
            segment_num=segment_num,
            conjoined_id=None,
            conjoined_num=None,
            conjoined_complete=None,
            file_size=0,
            file_adler32=None,
            file_hash=None,
            file_tombstone=True,
            handoff_node_id=None
        )
        _insert_segment_tombstone_row(self._connection, segment_row)

