# -*- coding: utf-8 -*-
"""
writer.py

Manage writing segment values to disk
"""
import logging
import os
import psycopg2

from tools.data_definitions import segment_sequence_template, \
        parse_timestamp_repr, \
        segment_status_active, \
        segment_status_tombstone
from tools.file_space import find_least_volume_space_id
from data_writer.output_value_file import OutputValueFile

_max_value_file_size = int(os.environ.get(
    "NIMBUS_IO_MAX_VALUE_FILE_SIZE", str(1024 * 1024 * 1024))
)

def _insert_conjoined_row(connection, conjoined_dict):
    connection.execute("""
        insert into nimbusio_node.conjoined (
            collection_id, key, unified_id, create_timestamp, handoff_node_id
        ) values (
            %(collection_id)s, 
            %(key)s, 
            %(unified_id)s, 
            %(create_timestamp)s::timestamp,
            %(handoff_node_id)s
        )""", conjoined_dict)                   

def _set_conjoined_abort_timestamp(connection, conjoined_dict):
    if conjoined_dict["handoff_node_id"] is None:
        connection.execute("""
            update nimbusio_node.conjoined 
            set abort_timestamp = %(abort_timestamp)s::timestamp
            where collection_id = %(collection_id)s
            and key = %(key)s
            and unified_id = %(unified_id)s
            and handoff_node_id is None
            """, conjoined_dict)                   
    else:
        connection.execute("""
            update nimbusio_node.conjoined 
            set abort_timestamp = %(abort_timestamp)s::timestamp
            where collection_id = %(collection_id)s
            and key = %(key)s
            and unified_id = %(unified_id)s
            and handoff_node_id = %(handoff_node_id)s
            """, conjoined_dict)                   

def _set_conjoined_complete_timestamp(connection, conjoined_dict):
    if conjoined_dict["handoff_node_id"] is None:
        connection.execute("""
            update nimbusio_node.conjoined 
            set complete_timestamp = %(complete_timestamp)s::timestamp
            where collection_id = %(collection_id)s
            and key = %(key)s
            and unified_id = %(unified_id)s
            and handoff_node_id is null
            """, conjoined_dict) 
    else:
        connection.execute("""
            update nimbusio_node.conjoined 
            set complete_timestamp = %(complete_timestamp)s::timestamp
            where collection_id = %(collection_id)s
            and key = %(key)s
            and unified_id = %(unified_id)s
            and handoff_node_id = %(handoff_node_id)s
            """, conjoined_dict) 

def _insert_new_segment_row(
    connection,
    collection_id, 
    unified_id,
    key, 
    timestamp, 
    conjoined_part,
    segment_num,
    source_node_id,
    handoff_node_id
):
    """
    Insert a new segment row in 'A'ctive status and return the id
    """
    return connection.execute_and_return_id("""
        insert into nimbusio_node.segment (
            collection_id,
            key,
            status,
            unified_id,
            timestamp,
            segment_num,
            conjoined_part,
            source_node_id,
            handoff_node_id
        ) values (
            %(collection_id)s,
            %(key)s,
            %(status)s,
            %(unified_id)s,
            %(timestamp)s::timestamp,
            %(segment_num)s,
            %(conjoined_part)s,
            %(source_node_id)s,
            %(handoff_node_id)s
        ) returning id""", {
            "collection_id"         : collection_id,
            "key"                   : key,
            "status"                : segment_status_active,
            "unified_id"            : unified_id,
            "timestamp"             : timestamp,
            "conjoined_part"        : conjoined_part,
            "segment_num"           : segment_num,
            "source_node_id"        : source_node_id,
            "handoff_node_id"       : handoff_node_id,
        }
    )

def _insert_segment_tombstone_row(
    connection,
    collection_id, 
    key, 
    unified_id,
    timestamp,
    segment_num,
    unified_id_to_delete,
    source_node_id,
    handoff_node_id
):
    """
    Insert one segment entry, with status set to (T)ombstone
    Set delete_timestamp on all conjoined rows for this key
    that are older than this tombstone
    """
    connection.execute("""
        insert into nimbusio_node.segment (
            collection_id,
            key,
            status,
            unified_id,
            timestamp,
            segment_num,
            file_tombstone_unified_id,
            source_node_id,
            handoff_node_id
        ) values (
            %(collection_id)s,
            %(key)s,
            %(status)s,
            %(unified_id)s,
            %(timestamp)s::timestamp,
            %(segment_num)s,
            %(file_tombstone_unified_id)s,
            %(source_node_id)s,
            %(handoff_node_id)s
        )""", {
            "collection_id"             : collection_id,
            "key"                       : key,
            "status"                    : segment_status_tombstone,
            "unified_id"                : unified_id,
            "timestamp"                 : timestamp,
            "segment_num"               : segment_num,
            "file_tombstone_unified_id" : unified_id_to_delete,
            "source_node_id"            : source_node_id,
            "handoff_node_id"           : handoff_node_id,
        }
    )
    connection.execute("""
        update nimbusio_node.conjoined 
        set delete_timestamp = %(timestamp)s::timestamp
        where collection_id = %(collection_id)s
          and key = %(key)s
          and (   (%(unified_id_to_delete)s is not null 
                   and unified_id = %(unified_id_to_delete)s)
               or (%(unified_id_to_delete)s is null
                   and unified_id < %(unified_id)s)
          )
        """, {
            "collection_id"             : collection_id,
            "key"                       : key,
            "unified_id"                : unified_id,
            "timestamp"                 : timestamp,
            "unified_id_to_delete"      : unified_id_to_delete,
        }
    )

def _cancel_segment_rows(connection, source_node_id, timestamp):
    """
    cancel all segment rows 
       * from a specifiic source node
       * are in active status 
       * with a timestamp earlier than the specified time. 
    This is triggered by a web server restart
    """
    connection.execute("""
        update nimbusio_node.segment
        set status = 'C'
        where source_node_id = %s 
        and status = 'A' 
        and timestamp < %s::timestamp
    """, [source_node_id, timestamp, ])

def _cancel_segment_row(connection, unified_id, conjoined_part, segment_num):
    """
    cancel a specific archive, presumably one in progress
    """
    connection.execute("""
        update nimbusio_node.segment
        set status = 'C'
        where unified_id = %(unified_id)s
        and conjoined_part = %(conjoined_part)s
        and segment_num = %(segment_num)s
        """, {"unified_id"       : unified_id, 
              "conjoined_part"   : conjoined_part,
              "segment_num"      : segment_num})

def _insert_segment_sequence_row(connection, segment_sequence_row):
    """
    Insert one segment_sequence entry
    """
    connection.execute("""
        insert into nimbusio_node.segment_sequence (
            "collection_id",
            "segment_id",
            "zfec_padding_size",
            "value_file_id",
            "sequence_num",
            "value_file_offset",
            "size",
            "hash",
            "adler32"
        ) values (
            %(collection_id)s,
            %(segment_id)s,
            %(zfec_padding_size)s,
            %(value_file_id)s,
            %(sequence_num)s,
            %(value_file_offset)s,
            %(size)s,
            %(hash)s,
            %(adler32)s
        )
    """, segment_sequence_row._asdict())

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

class Writer(object):
    """
    Manage writing segment values to disk
    """
    def __init__(self, 
                 connection, 
                 file_space_info, 
                 repository_path, 
                 active_segments, 
                 completions
    ):
        self._log = logging.getLogger("Writer")
        self._connection = connection
        self._file_space_info = file_space_info
        self._repository_path = repository_path
        self._active_segments = active_segments
        self._completions = completions
        
        space_id = find_least_volume_space_id("journal", self._file_space_info)

        # open a new value file at startup
        self._value_file = OutputValueFile(self._connection, 
                                           space_id, 
                                           self._repository_path)

    @property
    def value_file_hash(self):
        """
        return the hash of the currently open value file
        """
        assert self._value_file is not None
        return hash(self._value_file)

    def sync_value_file(self):
        """
        sync the current value file
        """
        assert self._value_file is not None
        self._value_file.sync()
        # at this point we can complete all pending archives

        self._connection.begin_transaction()
        try:
            for completion in self._completions:
                completion.pre_commit_process()
        except Exception:
            self._log.exception("sync_value_file")
            self._connection.rollback()
            raise
        self._connection.commit()

        for completion in self._completions:
            completion.post_commit_process()

        self._completions[:] = []

    @property
    def value_file_is_synced(self):
        assert self._value_file is not None
        return self._value_file.is_synced 

    def close(self):
        assert self._value_file is not None
        self.sync_value_file()
        self._value_file.close()
        self._value_file = None

    def start_new_segment(
        self, 
        collection_id, 
        key, 
        unified_id,
        timestamp_repr, 
        conjoined_part,
        segment_num,
        source_node_id,
        handoff_node_id,
    ):
        """
        Initiate storing a segment of data for a file
        """
        segment_key = (unified_id, conjoined_part, segment_num, )
        self._log.info("start_new_segment %s %s %s %s %s %s %s" % (
            collection_id, 
            key, 
            unified_id, 
            timestamp_repr, 
            conjoined_part,
            segment_num, 
            source_node_id,
        ))
        if segment_key in self._active_segments:
            raise ValueError("duplicate segment %s" % (segment_key, ))

        timestamp = parse_timestamp_repr(timestamp_repr)

        self._active_segments[segment_key] = {
            "segment-id" : _insert_new_segment_row(self._connection,
                                                   collection_id, 
                                                   unified_id,
                                                   key, 
                                                   timestamp, 
                                                   conjoined_part,
                                                   segment_num,
                                                   source_node_id,
                                                   handoff_node_id),
        }

    def store_sequence(
        self, 
        collection_id, 
        key, 
        unified_id,
        timestamp_repr, 
        conjoined_part,
        segment_num, 
        segment_size,
        zfec_padding_size,
        segment_md5_digest,
        segment_adler32,
        sequence_num, 
        data
    ):
        """
        store one piece (sequence) of segment data
        """
        segment_key = (unified_id, conjoined_part, segment_num, )
        self._log.info("store_sequence %s %s %s %s %s: %s (%s)" % (
            collection_id, 
            key, 
            unified_id,
            timestamp_repr, 
            segment_num, 
            sequence_num,
            segment_size
        ))
        segment_entry = self._active_segments[segment_key]

        # if this write would put us over the max size,
        # start a new output value file
        if self._value_file.size + segment_size > _max_value_file_size:
            self._value_file.close()
            space_id = find_least_volume_space_id("journal",
                                                  self._file_space_info)
            self._value_file = OutputValueFile(self._connection, 
                                               space_id,
                                               self._repository_path)

        segment_sequence_row = segment_sequence_template(
            collection_id=collection_id,
            segment_id=segment_entry["segment-id"],
            zfec_padding_size=zfec_padding_size,
            value_file_id=self._value_file.value_file_id,
            sequence_num=sequence_num,
            value_file_offset=self._value_file.size,
            size=segment_size,
            hash=psycopg2.Binary(segment_md5_digest),
            adler32=segment_adler32,
        )

        self._value_file.write_data_for_one_sequence(
            collection_id, segment_entry["segment-id"], data
        )

        _insert_segment_sequence_row(self._connection, segment_sequence_row)

    def set_tombstone(
        self, 
        collection_id, 
        key, 
        unified_id_to_delete,
        unified_id, 
        timestamp, 
        segment_num, 
        source_node_id,
        handoff_node_id
    ):
        """
        mark a key as deleted
        """
        _insert_segment_tombstone_row(
            self._connection,
            collection_id, 
            key, 
            unified_id,
            timestamp, 
            segment_num,
            unified_id_to_delete,
            source_node_id,
            handoff_node_id
        )

    def cancel_active_archives_from_node(self, source_node_id, timestamp):
        """
        cancel all segment rows 
           * from a specifiic source node
           * are in active status 
           * with a timestamp earlier than the specified time. 
        This is triggered by a web server restart
        """
        _cancel_segment_rows(self._connection, source_node_id, timestamp)

    def cancel_active_archive(self, unified_id, conjoined_part, segment_num):
        """
        cancel an archive that is in progress, presumably due to failure
        at the web server
        """
        segment_key = (unified_id, conjoined_part, segment_num, )
        self._log.info("cancel_active_archive %s" % (segment_key, ))
        # 2012-02-27 dougfort -- there is a race condition where the web
        # server sends out cancellations on an archive that has completed
        # because it hasn't reveived the final message yet
        try:
            self._active_segments.pop(segment_key)
        except KeyError:
            pass
        
        _cancel_segment_row(self._connection, 
                            unified_id, 
                            conjoined_part, 
                            segment_num)

    def start_conjoined_archive(
        self, collection_id, key, unified_id, timestamp, handoff_node_id
    ):
        """
        start a conjoined archive
        """
        conjoined_dict = {
            "collection_id"     : collection_id,
            "key"               : key,
            "unified_id"        : unified_id,
            "create_timestamp"  : timestamp,
            "handoff_node_id"   : handoff_node_id,
        }
        _insert_conjoined_row(self._connection, conjoined_dict)

    def abort_conjoined_archive(
        self, collection_id, key, unified_id, timestamp, handoff_node_id
    ):
        """
        mark a conjoined archive as aborted
        """
        conjoined_dict = {
            "collection_id"     : collection_id,
            "key"               : key,
            "unified_id"        : unified_id,
            "abort_timestamp"   : timestamp,
            "handoff_node_id"   : handoff_node_id,
        }
        _set_conjoined_abort_timestamp(self._connection, conjoined_dict)

    def finish_conjoined_archive(
        self, collection_id, key, unified_id, timestamp, handoff_node_id,
    ):
        """
        mark a conjoined archive as finished
        """
        conjoined_dict = {
            "collection_id"      : collection_id,
            "key"                : key,
            "unified_id"         : unified_id,
            "complete_timestamp" : timestamp,
            "handoff_node_id"    : handoff_node_id,
        }
        _set_conjoined_complete_timestamp(self._connection, conjoined_dict)

