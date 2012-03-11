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
        meta_row_template, \
        segment_status_active, \
        segment_status_cancelled, \
        segment_status_final, \
        segment_status_tombstone
from data_writer.output_value_file import OutputValueFile

_max_value_file_size = int(os.environ.get(
    "NIMBUS_IO_MAX_VALUE_FILE_SIZE", str(1024 * 1024 * 1024))
)

def _insert_conjoined_row(connection, conjoined_dict):
    connection.execute("""
        insert into nimbusio_node.conjoined (
            collection_id, key, unified_id, create_timestamp
        ) values (
            %(collection_id)s, 
            %(key)s, 
            %(unified_id)s, 
            %(create_timestamp)s::timestamp
        )""", conjoined_dict)                   
    connection.commit()

def _set_conjoined_abort_timestamp(connection, conjoined_dict):
    connection.execute("""
        update nimbusio_node.conjoined 
        set abort_timestamp = %(abort_timestamp)s::timestamp
        where collection_id = %(collection_id)s
        and key = %(key)s
        and unified_id = %(unified_id)s
        """, conjoined_dict)                   
    connection.commit()

def _set_conjoined_complete_timestamp(connection, conjoined_dict):
    connection.execute("""
        update nimbusio_node.conjoined 
        set complete_timestamp = %(complete_timestamp)s::timestamp
        where collection_id = %(collection_id)s
        and key = %(key)s
        and unified_id = %(unified_id)s
        """, conjoined_dict)                   
    connection.commit()

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

    connection.commit()

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
    Insert one segment entry, with default row id
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
    connection.commit()

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
    connection.commit()

def _cancel_segment_row(connection, segment_id):
    """
    cancel a specific archive, presumably one in progress
    """
    connection.execute("""
        update nimbusio_node.segment
        set status = 'C'
        where id = %s 
        and status = 'A' 
    """, [segment_id, ])
    connection.commit()

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

def _purge_handoff_source(
    connection, collection_id, unified_id, handoff_node_id
):
    """
    remove handoff source from local database
    """
    # XXX review this is going to be a slow query. any way to get key= in here
    # and use the index?
    connection.execute("""
        delete from nimbusio_node.segment_sequence 
        where segment_id = (
            select id from nimbusio_node.segment
            where collection_id = %s 
            and unified_id = %s
            and handoff_node_id = %s
        );
        delete from nimbusio_node.segment
        where collection_id = %s 
        and unified_id = %s
        and handoff_node_id = %s;
    """, [collection_id,
          unified_id,
          handoff_node_id,
          collection_id,
          unified_id,
          handoff_node_id])
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
        data,
        fsync_task_id,
        fsync_task_queue
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
            # so we want to be sure there are no fsyncs in progress
            # for this file before we close it. but closing it also updates the
            # database. So I guess we need to insert a task into the fsync
            # thread queue that just notifies the main thread when there are no
            # more fsyncs for this file number, so the main thread can dispatch
            # a task to close the file.
            fsync_task_queue.put(
                # XXX: does a bound method hold the ref to the object, and keep
                # it from getting GC'd? or does it only hold a weak reference? 
                ('queue-when-all-fsyncs-complete', self._value_file.close, ))
            self._value_file = OutputValueFile(
                self._connection, self._repository_path
            )

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

        # at this point we want to queue a task for the background
        # thread 
        fsync_queue.put(
            ('fsync-fileno', fsync_task_id, self._value_file.fileno,))

        # we work on the database while the background thread writes to disk
        _insert_segment_sequence_row(self._connection, segment_sequence_row)



    def finish_new_segment(
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
        self._log.info("cancel_active_archive %s %s" % (
            unified_id, segment_num
        ))
        # 2012-02-27 dougfort -- there is a race condition where the web
        # server sends out cancellations on an archive that has completed
        # because it hasn't reveived the final message yet
        try:
            segment_entry = self._active_segments.pop(segment_key)
        except KeyError:
            pass
        else:
            _cancel_segment_row(self._connection, segment_entry["segment-id"])

    def purge_handoff_source(
        self, collection_id, unified_id, handoff_node_id
    ):
        """
        delete rows for a handoff source
        """
        _purge_handoff_source(
            self._connection, 
            collection_id, 
            unified_id, 
            handoff_node_id
        )

    def start_conjoined_archive(
        self, collection_id, key, unified_id, timestamp
    ):
        """
        start a conjoined archive
        """
        conjoined_dict = {
            "collection_id"     : collection_id,
            "key"               : key,
            "unified_id"        : unified_id,
            "create_timestamp"  : timestamp
        }
        _insert_conjoined_row(self._connection, conjoined_dict)

    def abort_conjoined_archive(
        self, collection_id, key, unified_id, timestamp
    ):
        """
        mark a conjoined archive as aborted
        """
        conjoined_dict = {
            "collection_id"     : collection_id,
            "key"               : key,
            "unified_id"        : unified_id,
            "abort_timestamp"   : timestamp
        }
        _set_conjoined_abort_timestamp(self._connection, conjoined_dict)

    def finish_conjoined_archive(
        self, collection_id, key, unified_id, timestamp
    ):
        """
        mark a conjoined archive as finished
        """
        conjoined_dict = {
            "collection_id"      : collection_id,
            "key"                : key,
            "unified_id"         : unified_id,
            "complete_timestamp" : timestamp
        }
        _set_conjoined_complete_timestamp(self._connection, conjoined_dict)

