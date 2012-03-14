# -*- coding: utf-8 -*-
"""
post_sync_completion.py

Actions to be taken to complete an archive after the last value file
is fsync'd
"""
from base64 import b64decode
import logging
import psycopg2

from tools.data_definitions import parse_timestamp_repr, \
        meta_row_template, \
        segment_status_final, \
        nimbus_meta_prefix

_sizeof_nimbus_meta_prefix = len(nimbus_meta_prefix)

def _extract_meta(message):
    """
    build a dict of meta data, with our meta prefix stripped off
    """
    meta_dict = dict()
    for key in message:
        if key.startswith(nimbus_meta_prefix):
            converted_key = key[_sizeof_nimbus_meta_prefix:]
            meta_dict[converted_key] = message[key]
    return meta_dict

def _finalize_segment_row(
    connection, segment_id, file_size, file_adler32, file_hash, meta_rows
):
    """
    Update segment row, set status to 'F'inal, include
    associated meta rows
    """
    connection.execute("""
        update nimbusio_node.segment 
        set status = %(status)s,
            file_size = %(file_size)s,
            file_adler32 = %(file_adler32)s,
            file_hash = %(file_hash)s
        where id = %(segment_id)s
    """, {
        "segment_id"    : segment_id,
        "status"        : segment_status_final,
        "file_size"     : file_size,
        "file_adler32"  : file_adler32,
        "file_hash"     : psycopg2.Binary(file_hash),
    })

    for meta_row in meta_rows:
        meta_row_dict = meta_row._asdict()
        connection.execute("""
            insert into nimbusio_node.meta (
                collection_id,
                segment_id,
                meta_key,
                meta_value,
                timestamp
            ) values (
                %(collection_id)s,
                %(segment_id)s,
                %(meta_key)s,
                %(meta_value)s,
                %(timestamp)s::timestamp
            )
        """, meta_row_dict)            

    # 2012-03-14 dougfort -- assume all completions are run in a
    # transaction wioht the caller handling the database commit

class PostSyncCompletion(object):
    """
    Actions to be taken to complete an archive after the last value file
    is fsync'd
    """
    def __init__(self, 
                 connection,
                 resilient_server,
                 active_segments,
                 archive_message, 
                 reply_message):
        self._log = logging.getLogger("PostSyncCompletion")

        self._connection = connection
        self._resilient_server = resilient_server
        self._active_segments = active_segments
        self._archive_message = archive_message
        self._reply_message = reply_message

    def complete_archive(self):
        """
        finalize the segment row
        send archive_key_file_reply message to the caller
        """
        self._finish_new_segment(
            self._archive_message["collection-id"], 
            self._archive_message["unified-id"],
            self._archive_message["timestamp-repr"],
            self._archive_message["conjoined-part"],
            self._archive_message["segment-num"],
            self._archive_message["file-size"],
            self._archive_message["file-adler32"],
            b64decode(self._archive_message["file-hash"]),
            _extract_meta(self._archive_message),
        )

        self._resilient_server.send_reply(self._reply_message)

    def _finish_new_segment(
        self, 
        collection_id,
        unified_id,
        timestamp_repr,
        conjoined_part,
        segment_num,
        file_size,
        file_adler32,
        file_hash,
        meta_dict,
    ): 
        """
        finalize storing one segment of data for a file
        """
        segment_key = (unified_id, conjoined_part, segment_num, )
        self._log.info("finish_new_segment %s %s" % (
            unified_id, 
            segment_num, 
        ))
        segment_entry = self._active_segments.pop(segment_key)

        timestamp = parse_timestamp_repr(timestamp_repr)

        meta_rows = list()
        for meta_key, meta_value in meta_dict.items():
            meta_row = meta_row_template(
                collection_id=collection_id,
                segment_id=segment_entry["segment-id"],
                meta_key=meta_key,
                meta_value=meta_value,
                timestamp=timestamp
            )
            meta_rows.append(meta_row)

        _finalize_segment_row(
            self._connection, 
            segment_entry["segment-id"],
            file_size, 
            file_adler32, 
            file_hash, 
            meta_rows
        )
    
