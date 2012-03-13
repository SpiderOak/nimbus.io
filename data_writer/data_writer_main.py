# -*- coding: utf-8 -*-
"""
data_writer_main.py

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
import sys
import time

import zmq

import Statgrabber

from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.event_push_client import EventPushClient, exception_event
from tools.priority_queue import PriorityQueue
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process
from tools.database_connection import get_node_local_connection, \
        get_central_connection
from tools.data_definitions import parse_timestamp_repr, \
        nimbus_meta_prefix
from web_server.central_database_util import get_cluster_row, \
        get_node_rows

from data_writer.output_value_file import mark_value_files_as_closed
from data_writer.writer import Writer
from data_writer.stats_reporter import StatsReporter
from data_writer.sync_manager import SyncManager
from data_writer.post_sync_completion import PostSyncCompletion

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/nimbusio_data_writer_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_data_writer_address = os.environ.get(
    "NIMBUSIO_DATA_WRITER_ADDRESS",
    "tcp://127.0.0.1:8100"
)
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]

def _handle_archive_key_entire(state, message, data):
    log = logging.getLogger("_handle_archive_key_entire")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    sequence_num = 0

    reply = {
        "message-type"  : "archive-key-final-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # we expect a list of blocks, but if the data is smaller than 
    # block size, we get back a string
    if type(data) != list:
        data = [data, ]

    segment_data = "".join(data)

    if len(segment_data) != message["segment-size"]:
        error_message = "size mismatch (%s != %s) %s %s %s %s" % (
            len(segment_data),
            message["segment-size"],
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("size-mismatch", error_message)  
        reply["result"] = "size-mismatch"
        reply["error-message"] = "segment size does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
    segment_md5 = hashlib.md5()
    segment_md5.update(segment_data)
    if segment_md5.digest() != expected_segment_md5_digest:
        error_message = "md5 mismatch %s %s %s %s" % (
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("md5-mismatch", error_message)  
        reply["result"] = "md5-mismatch"
        reply["error-message"] = "segment md5 does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    source_node_id = state["node-id-dict"][message["source-node-name"]]
    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].start_new_segment(
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        message["timestamp-repr"],
        message["conjoined-part"],
        message["segment-num"],
        source_node_id,
        handoff_node_id
    )

    state["writer"].store_sequence(
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
        segment_data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

    reply["result"] = "success"
    # we don't send the reply until all value file dependencies have
    # been synced
    state["completions"].append(
        PostSyncCompletion(state["database-connection"],
                           state["resilient-server"],
                           state["active-segments"],
                           message,
                           reply)
    )

def _handle_archive_key_start(state, message, data):
    log = logging.getLogger("_handle_archive_key_start")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    reply = {
        "message-type"  : "archive-key-start-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # we expect a list of blocks, but if the data is smaller than 
    # block size, we get back a string
    if type(data) != list:
        data = [data, ]

    segment_data = "".join(data)

    if len(segment_data) != message["segment-size"]:
        error_message = "size mismatch (%s != %s) %s %s %s %s" % (
            len(segment_data),
            message["segment-size"],
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("size-mismatch", error_message)  
        reply["result"] = "size-mismatch"
        reply["error-message"] = "segment size does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
    segment_md5 = hashlib.md5()
    segment_md5.update(segment_data)
    if segment_md5.digest() != expected_segment_md5_digest:
        error_message = "md5 mismatch %s %s %s %s" % (
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("md5-mismatch", error_message)  
        reply["result"] = "md5-mismatch"
        reply["error-message"] = "segment md5 does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    source_node_id = state["node-id-dict"][message["source-node-name"]]
    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].start_new_segment(
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        message["timestamp-repr"],
        message["conjoined-part"],
        message["segment-num"],
        source_node_id,
        handoff_node_id
    )

    state["writer"].store_sequence(
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
        segment_data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_next(state, message, data):
    log = logging.getLogger("_handle_archive_key_next")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    reply = {
        "message-type"  : "archive-key-next-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # we expect a list of blocks, but if the data is smaller than 
    # block size, we get back a string
    if type(data) != list:
        data = [data, ]

    segment_data = "".join(data)

    if len(segment_data) != message["segment-size"]:
        error_message = "size mismatch (%s != %s) %s %s %s %s" % (
            len(segment_data),
            message["segment-size"],
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("size-mismatch", error_message)  
        reply["result"] = "size-mismatch"
        reply["error-message"] = "segment size does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
    segment_md5 = hashlib.md5()
    segment_md5.update(segment_data)
    if segment_md5.digest() != expected_segment_md5_digest:
        error_message = "md5 mismatch %s %s %s %s" % (
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("md5-mismatch", error_message)  
        reply["result"] = "md5-mismatch"
        reply["error-message"] = "segment md5 does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    state["writer"].store_sequence(
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
        segment_data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_final(state, message, data):
    log = logging.getLogger("_handle_archive_key_final")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    reply = {
        "message-type"  : "archive-key-final-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # we expect a list of blocks, but if the data is smaller than 
    # block size, we get back a string
    if type(data) != list:
        data = [data, ]

    segment_data = "".join(data)

    if len(segment_data) != message["segment-size"]:
        error_message = "size mismatch (%s != %s) %s %s %s %s" % (
            len(segment_data),
            message["segment-size"],
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("size-mismatch", error_message)  
        reply["result"] = "size-mismatch"
        reply["error-message"] = "segment size does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    expected_segment_md5_digest = b64decode(message["segment-md5-digest"])
    segment_md5 = hashlib.md5()
    segment_md5.update(segment_data)
    if segment_md5.digest() != expected_segment_md5_digest:
        error_message = "md5 mismatch %s %s %s %s" % (
            message["collection-id"], 
            message["key"], 
            message["timestamp-repr"],
            message["segment-num"]
        )
        log.error(error_message)
        state["event-push-client"].error("md5-mismatch", error_message)  
        reply["result"] = "md5-mismatch"
        reply["error-message"] = "segment md5 does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    state["writer"].store_sequence(
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
        segment_data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(segment_data))

    reply["result"] = "success"
    # we don't send the reply until all value file dependencies have
    # been synced
    state["completions"].append(
        PostSyncCompletion(state["database-connection"],
                           state["resilient-server"],
                           state["active-segments"],
                           message,
                           reply)
    )

def _handle_archive_key_cancel(state, message, _data):
    log = logging.getLogger("_handle_archive_key_cancel")
    log.info("%s %s" % ( message["unified-id"], message["segment-num"],))
    state["writer"].cancel_active_archive(
        message["unified-id"], 
        message["conjoined-part"], 
        message["segment-num"],
    )

def _handle_destroy_key(state, message, _data):
    log = logging.getLogger("_handle_destroy_key")
    log.info("%s %s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["unified-id-to-delete"],
        message["unified-id"],
        message["segment-num"]
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])
    source_node_id = state["node-id-dict"][message["source-node-name"]]
    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].set_tombstone(
        message["collection-id"], 
        message["key"], 
        message["unified-id-to-delete"],
        message["unified-id"],
        timestamp,
        message["segment-num"],
        source_node_id,
        handoff_node_id
    )

    reply = {
        "message-type"  : "destroy-key-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_purge_handoff_source(state, message, _data):
    log = logging.getLogger("_handle_purge_handoff_source")
    log.info("%s %s %s" % (
        message["collection-id"], 
        message["unified-id"], 
        message["handoff-node-id"],
    ))

    state["writer"].purge_handoff_source(
        message["collection-id"], 
        message["unified-id"],
        message["handoff-node-id"]
    )

def _handle_start_conjoined_archive(state, message, _data):
    log = logging.getLogger("_handle_start_conjoined_archive")
    log.info("%r %r %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].start_conjoined_archive(
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        timestamp
    )

    reply = {
        "message-type"  : "start-conjoined-archive-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_abort_conjoined_archive(state, message, _data):
    log = logging.getLogger("_handle_abort_conjoined_archive")
    log.info("%r %r %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].abort_conjoined_archive(
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        timestamp
    )

    reply = {
        "message-type"  : "abort-conjoined-archive-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_finish_conjoined_archive(state, message, _data):
    log = logging.getLogger("_handle_finish_conjoined_archive")
    log.info("%r %r %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].finish_conjoined_archive(
        message["collection-id"], 
        message["key"], 
        message["unified-id"],
        timestamp
    )

    reply = {
        "message-type"  : "finish-conjoined-archive-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_web_server_start(state, message, _data):
    log = logging.getLogger("_handle_web_server_start")
    log.info("%s %s %s" % (message["unified-id"], 
                           message["timestamp-repr"],
                           message["source-node-name"]))

    source_node_id = state["node-id-dict"][message["source-node-name"]]
    timestamp = parse_timestamp_repr(message["timestamp-repr"])
    state["writer"].cancel_active_archives_from_node(
        source_node_id, timestamp 
    )

_dispatch_table = {
    "archive-key-entire"        : _handle_archive_key_entire,
    "archive-key-start"         : _handle_archive_key_start,
    "archive-key-next"          : _handle_archive_key_next,
    "archive-key-final"         : _handle_archive_key_final,
    "archive-key-cancel"        : _handle_archive_key_cancel,
    "destroy-key"               : _handle_destroy_key,
    "purge-handoff-source"      : _handle_purge_handoff_source,
    "start-conjoined-archive"   : _handle_start_conjoined_archive,
    "abort-conjoined-archive"   : _handle_abort_conjoined_archive,
    "finish-conjoined-archive"  : _handle_finish_conjoined_archive,
    "web-server-start"          : _handle_web_server_start,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "resilient-server"      : None,
        "event-push-client"     : None,
        "stats-reporter"        : None,
        "receive-queue"         : PriorityQueue(),
        "queue-dispatcher"      : None,
        "writer"                : None,
        "database-connection"   : None,
        "cluster-row"           : None,
        "node-rows"             : None,
        "node-id-dict"          : None,
        "active-segments"       : dict(),
        "completions"           : list(),
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "data_writer"
    )

    log.info("binding resilient-server to %s" % (_data_writer_address, ))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _data_writer_address,
        state["receive-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    central_connection = get_central_connection()
    state["cluster-row"] = get_cluster_row(central_connection)
    state["node-rows"] = get_node_rows(
        central_connection, state["cluster-row"].id
    )
    central_connection.close()

    state["node-id-dict"] = dict(
        [(node_row.name, node_row.id, ) for node_row in state["node-rows"]]
    )

    state["database-connection"] = get_node_local_connection()

    # Ticket #1646 mark output value files as closed at startup
    mark_value_files_as_closed(state["database-connection"])

    state["writer"] = Writer(state["database-connection"], 
                             _repository_path,
                             state["active-segments"],
                             state["completions"])

    state["sync-manager"] = SyncManager(state["writer"])

    state["stats-reporter"] = StatsReporter(state)

    state["event-push-client"].info("program-start", "data_writer starts")  

    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["stats-reporter"].run, state["stats-reporter"].next_run(), ), 
        (state["sync-manager"].run, state["sync-manager"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping resilient server")
    state["resilient-server"].close()
    state["event-push-client"].close()

    state["zmq-context"].term()

    state["writer"].close()
    state["database-connection"].close()

    if len(state["completions"]) > 0:
        log.warn("{0} PostSyncCompletion's lost in teardown".format(
            len(state["completions"])))

    if len(state["active-segments"]) > 0:
        log.warn("{0} active-segments at teardown".format(
            len(state["active-segments"])))

    log.debug("teardown complete")

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ],
            exception_action=exception_event
        )
    )


