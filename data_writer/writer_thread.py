# -*- coding: utf-8 -*-
"""
writer_thread.py

Stores received segments (1 for each sequence)
in the incoming directory with a temp extension.
When final segment is received
fsyncs temp data file
renames into place,
fsyncs the directory into which the file was renamed
sends message to the database server to record key as stored.
ACK back to to requestor includes size (from the database server)
of any previous key this key supersedes (for space accounting.)
"""
from base64 import b64decode
import hashlib
import logging
import os
import queue
from threading import Thread
import sys

import zmq

import Statgrabber

from tools.file_space import load_file_space_info, file_space_sanity_check
from tools.database_connection import get_node_local_connection
from tools.push_client import PUSHClient
from tools.data_definitions import parse_timestamp_repr

from data_writer.output_value_file import mark_value_files_as_closed
from data_writer.writer import Writer
from data_writer.post_sync_completion import PostSyncCompletion

writer_thread_reply_address = "inproc://writer_thread_reply"

_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_queue_timeout = 1.0

class WriterThread(Thread):
    """
    manage writes to filesystem
    """
    def __init__(self, halt_event, node_id_dict, message_queue):
        Thread.__init__(self, name="WriterThread")
        self._halt_event = halt_event
        self._node_id_dict = node_id_dict
        self._message_queue = message_queue
        self._database_connection = get_node_local_connection()
        self._zmq_context = zmq.Context()
        self._active_segments = dict()
        self._completions = list()
        self._writer = None

        self._reply_pusher = PUSHClient(self._zmq_context,
                                        writer_thread_reply_address)

        self._dispatch_table = {
            "archive-key-entire"        : self._handle_archive_key_entire,
            "archive-key-start"         : self._handle_archive_key_start,
            "archive-key-next"          : self._handle_archive_key_next,
            "archive-key-final"         : self._handle_archive_key_final,
            "archive-key-cancel"        : self._handle_archive_key_cancel,
            "destroy-key"               : self._handle_destroy_key,
            "start-conjoined-archive"   : self._handle_start_conjoined_archive,
            "abort-conjoined-archive"   : self._handle_abort_conjoined_archive,
            "finish-conjoined-archive"  : self._handle_finish_conjoined_archive,
            "web-writer-start"          : self._handle_web_writer_start,
            "sync-value-file"           : self._handle_sync_value_file,
        }

    def run(self):
        log = logging.getLogger("WriterThread.run")
        try:
            self._run()
        except Exception:
            instance = sys.exc_info()[1]
            log.exception("unhandled exception in WriterThread")
            log.critical("unhandled exception in WriterThread {0}".format(
                instance))
            self._halt_event.set()
            return

    def _run(self):
        log = logging.getLogger("WriterThread._run")
        file_space_info = load_file_space_info(self._database_connection)
        file_space_sanity_check(file_space_info, _repository_path)

        # Ticket #1646 mark output value files as closed at startup
        mark_value_files_as_closed(self._database_connection)

        self._writer = Writer(self._database_connection,
                             file_space_info,
                             _repository_path,
                             self._active_segments,
                             self._completions)

        log.debug("start halt_event loop")
        while not self._halt_event.is_set():
            try:
                message, data = self._message_queue.get(block=True,
                                                        timeout=_queue_timeout)
            except queue.Empty:
                pass
            self._dispatch_table[message["message-type"]](message, data)
        log.debug("end halt_event loop")

        # 2012-03-27 dougfort -- we stop the data writer first because it is
        # going to sync the value file and run the post_sync operations
        log.debug("stopping data writer")
        self._writer.close()

        log.debug("term'ing zmq context")
        self._zmq_context.term()

        log.debug("closing database connection")
        self._database_connection.close()

        if len(self._completions) > 0:
            log.warn("{0} PostSyncCompletion's lost in teardown".format(
                len(self._completions)))

        if len(self._active_segments) > 0:
            log.warn("{0} active-segments at teardown".format(
                len(self._active_segments)))


    def _handle_archive_key_entire(self, message, data):
        log = logging.getLogger("_handle_archive_key_entire")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["timestamp-repr"],
            message["segment-num"]))

        sequence_num = 0

        reply = {
            "message-type"      : "archive-key-final-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : None,
            "error-message"     : None,
        }

        # we expect a list of blocks, but if the data is smaller than
        # block size, we get back a string
        if type(data) != list:
            data = [data, ]

        segment_data = "".join(data)

        if len(segment_data) != message["segment-size"]:
            error_message = "size mismatch ({0} != {1}) {2} {3} {4} {5}".format(
                len(segment_data),
                message["segment-size"],
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"])
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "size-mismatch"
            reply["error-message"] = \
                "segment size does not match expected value"
            self._reply_pusher.send(reply)
            return

        expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
        segment_md5 = hashlib.md5()
        segment_md5.update(segment_data)
        if segment_md5.digest() != expected_segment_md5_digest:
            error_message = "md5 mismatch {0} {1} {2} {3}".format(
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"])
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "md5-mismatch"
            reply["error-message"] = "segment md5 does not match expected value"
            self._reply_pusher.send(reply)
            return

        source_node_id = self._node_id_dict[message["source-node-name"]]
        if message["handoff-node-name"] is None:
            handoff_node_id = None
        else:
            handoff_node_id = self._node_id_dict[message["handoff-node-name"]]

        self._writer.start_new_segment(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
            message["conjoined-part"],
            message["segment-num"],
            source_node_id,
            handoff_node_id,
            message["user-request-id"]
        )

        self._writer.store_sequence(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
            message["conjoined-part"],
            message["segment-num"],
            message["segment-size"],
            message["zfec-padding-size"],
            expected_segment_md5_digest,
            message["segment-adler32"],
            sequence_num,
            segment_data,
            message["user-request-id"]
        )

        Statgrabber.accumulate('nimbusio_write_requests', 1)
        Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

        reply["result"] = "success"
        # we don't send the reply until all value file dependencies have
        # been synced
        self._completions.append(
            PostSyncCompletion(self._database_connection,
                               self._reply_pusher,
                               self._active_segments,
                               message,
                               reply)
        )

    def _handle_archive_key_start(self, message, data):
        log = logging.getLogger("_handle_archive_key_start")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["timestamp-repr"],
            message["segment-num"]))

        reply = {
            "message-type"      : "archive-key-start-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : None,
            "error-message"     : None,
        }

        # we expect a list of blocks, but if the data is smaller than
        # block size, we get back a string
        if type(data) != list:
            data = [data, ]

        segment_data = "".join(data)

        if len(segment_data) != message["segment-size"]:
            error_message = "size mismatch ({0} != {1}) {2} {3} {4} {5}".format(
                len(segment_data),
                message["segment-size"],
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"])
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "size-mismatch"
            reply["error-message"] = \
                "segment size does not match expected value"
            self._reply_pusher.send(reply)
            return

        expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
        segment_md5 = hashlib.md5()
        segment_md5.update(segment_data)
        if segment_md5.digest() != expected_segment_md5_digest:
            error_message = "md5 mismatch {0} {1} {2} {3}".format(
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"])
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "md5-mismatch"
            reply["error-message"] = "segment md5 does not match expected value"
            self._reply_pusher.send(reply)
            return

        source_node_id = self._node_id_dict[message["source-node-name"]]
        if message["handoff-node-name"] is None:
            handoff_node_id = None
        else:
            handoff_node_id = self._node_id_dict[message["handoff-node-name"]]

        self._writer.start_new_segment(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
            message["conjoined-part"],
            message["segment-num"],
            source_node_id,
            handoff_node_id,
            message["user-request-id"]
        )

        self._writer.store_sequence(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
            message["conjoined-part"],
            message["segment-num"],
            message["segment-size"],
            message["zfec-padding-size"],
            expected_segment_md5_digest,
            message["segment-adler32"],
            message["sequence-num"],
            segment_data,
            message["user-request-id"]
        )

        Statgrabber.accumulate('nimbusio_write_requests', 1)
        Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

        reply["result"] = "success"
        self._reply_pusher.send(reply)

    def _handle_archive_key_next(self, message, data):
        log = logging.getLogger("_handle_archive_key_next")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["timestamp-repr"],
            message["segment-num"]))

        reply = {
            "message-type"      : "archive-key-next-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : None,
            "error-message"     : None,
        }

        # we expect a list of blocks, but if the data is smaller than
        # block size, we get back a string
        if type(data) != list:
            data = [data, ]

        segment_data = "".join(data)

        if len(segment_data) != message["segment-size"]:
            error_message = "size mismatch ({0} != {1}) {2} {3} {4} {5}".format(
                len(segment_data),
                message["segment-size"],
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"]
            )
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "size-mismatch"
            reply["error-message"] = \
                "segment size does not match expected value"
            self._reply_pusher.send(reply)
            return

        expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
        segment_md5 = hashlib.md5()
        segment_md5.update(segment_data)
        if segment_md5.digest() != expected_segment_md5_digest:
            error_message = "md5 mismatch {0} {1} {2} {3}".format(
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"]
            )
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "md5-mismatch"
            reply["error-message"] = "segment md5 does not match expected value"
            self._reply_pusher.send(reply)
            return

        self._writer.store_sequence(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
            message["conjoined-part"],
            message["segment-num"],
            message["segment-size"],
            message["zfec-padding-size"],
            expected_segment_md5_digest,
            message["segment-adler32"],
            message["sequence-num"],
            segment_data,
            message["user-request-id"]
        )

        Statgrabber.accumulate('nimbusio_write_requests', 1)
        Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

        reply["result"] = "success"
        self._reply_pusher.send(reply)

    def _handle_archive_key_final(self, message, data):
        log = logging.getLogger("_handle_archive_key_final")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["timestamp-repr"],
            message["segment-num"]
        ))

        reply = {
            "message-type"      : "archive-key-final-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : None,
            "error-message"     : None,
        }

        # we expect a list of blocks, but if the data is smaller than
        # block size, we get back a string
        if type(data) != list:
            data = [data, ]

        segment_data = "".join(data)

        if len(segment_data) != message["segment-size"]:
            error_message = "size mismatch ({0} != {1}) {2} {3} {4} {4}".format(
                len(segment_data),
                message["segment-size"],
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"])
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "size-mismatch"
            reply["error-message"] = \
                "segment size does not match expected value"
            self._reply_pusher.send(reply)
            return

        expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
        segment_md5 = hashlib.md5()
        segment_md5.update(segment_data)
        if segment_md5.digest() != expected_segment_md5_digest:
            error_message = "md5 mismatch {0} {1} {2} {3}".format(
                message["collection-id"],
                message["key"],
                message["timestamp-repr"],
                message["segment-num"])
            log.error("request {0}: {1}".format(message["user-request-id"],
                                                error_message))
            reply["result"] = "md5-mismatch"
            reply["error-message"] = "segment md5 does not match expected value"
            self._reply_pusher.send(reply)
            return

        self._writer.store_sequence(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
            message["conjoined-part"],
            message["segment-num"],
            message["segment-size"],
            message["zfec-padding-size"],
            expected_segment_md5_digest,
            message["segment-adler32"],
            message["sequence-num"],
            segment_data,
            message["user-request-id"]
        )

        Statgrabber.accumulate('nimbusio_write_requests', 1)
        Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

        reply["result"] = "success"
        # we don't send the reply until all value file dependencies have
        # been synced
        self._completions.append(
            PostSyncCompletion(self._database_connection,
                               self._reply_pusher,
                               self._active_segments,
                               message,
                               reply)
        )

    def _handle_archive_key_cancel(self, message, _data):
        log = logging.getLogger("_handle_archive_key_cancel")
        log.info("request {0}: {1} {2}".format(
                 message["user-request-id"],
                 message["unified-id"],
                 message["segment-num"]))
        self._writer.cancel_active_archive(
            message["unified-id"],
            message["conjoined-part"],
            message["segment-num"],
            message["user-request-id"]
        )

    def _handle_destroy_key(self, message, _data):
        log = logging.getLogger("_handle_destroy_key")
        log.info("request {0}: {1} {2} {3} {4} {5}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["unified-id-to-delete"],
            message["unified-id"],
            message["segment-num"]
        ))

        timestamp = parse_timestamp_repr(message["timestamp-repr"])
        source_node_id = self._node_id_dict[message["source-node-name"]]
        if message["handoff-node-name"] is None:
            handoff_node_id = None
        else:
            handoff_node_id = self._node_id_dict[message["handoff-node-name"]]

        self._writer.set_tombstone(
            message["collection-id"],
            message["key"],
            message["unified-id-to-delete"],
            message["unified-id"],
            timestamp,
            message["segment-num"],
            source_node_id,
            handoff_node_id,
            message["user-request-id"]
        )

        reply = {
            "message-type"      : "destroy-key-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "unified-id"        : message["unified-id"],
            "result"            : "success",
            "error-message"     : None,
        }
        self._reply_pusher.send(reply)


    def _handle_start_conjoined_archive(self, message, _data):
        log = logging.getLogger("_handle_start_conjoined_archive")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],))

        timestamp = parse_timestamp_repr(message["timestamp-repr"])

        if "handoff-node-name" not in message or \
           message["handoff-node-name"] is None:
            handoff_node_id = None
        else:
            handoff_node_id = self._node_id_dict[message["handoff-node-name"]]

        self._writer.start_conjoined_archive(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            timestamp,
            handoff_node_id)

        reply = {
            "message-type"      : "start-conjoined-archive-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : "success",
            "error-message"     : None,
        }
        self._reply_pusher.send(reply)

    def _handle_abort_conjoined_archive(self, message, _data):
        log = logging.getLogger("_handle_abort_conjoined_archive")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
        ))

        timestamp = parse_timestamp_repr(message["timestamp-repr"])

        if "handoff-node-name" not in message or \
           message["handoff-node-name"] is None:
            handoff_node_id = None
        else:
            handoff_node_id = self._node_id_dict[message["handoff-node-name"]]

        self._writer.abort_conjoined_archive(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            timestamp,
            handoff_node_id)

        reply = {
            "message-type"      : "abort-conjoined-archive-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : "success",
            "error-message"     : None,
        }
        self._reply_pusher.send(reply)

    def _handle_finish_conjoined_archive(self, message, _data):
        log = logging.getLogger("_handle_finish_conjoined_archive")
        log.info("request {0}: {1} {2} {3} {4}".format(
            message["user-request-id"],
            message["collection-id"],
            message["key"],
            message["unified-id"],
            message["timestamp-repr"],
        ))

        timestamp = parse_timestamp_repr(message["timestamp-repr"])

        if "handoff-node-name" not in message or \
           message["handoff-node-name"] is None:
            handoff_node_id = None
        else:
            handoff_node_id = self._node_id_dict[message["handoff-node-name"]]

        self._writer.finish_conjoined_archive(
            message["collection-id"],
            message["key"],
            message["unified-id"],
            timestamp,
            handoff_node_id)

        reply = {
            "message-type"      : "finish-conjoined-archive-reply",
            "client-tag"        : message["client-tag"],
            "client-address"    : message["client-address"],
            "user-request-id"   : message["user-request-id"],
            "message-id"        : message["message-id"],
            "result"            : "success",
            "error-message"     : None,
        }
        self._reply_pusher.send(reply)

    def _handle_web_writer_start(self, message, _data):
        log = logging.getLogger("_handle_web_writer_start")
        log.info("{0} {1} {2}".format(message["unified_id"],
                                      message["timestamp_repr"],
                                      message["source_node_name"]))

        source_node_id = self._node_id_dict[message["source_node_name"]]
        timestamp = parse_timestamp_repr(message["timestamp_repr"])
        self._writer.cancel_active_archives_from_node(
            source_node_id, timestamp
        )

    def _handle_sync_value_file(self, _message, _data):
        self._writer.sync_value_file()
