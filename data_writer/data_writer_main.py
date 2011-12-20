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

from data_writer.writer import Writer
from data_writer.stats_reporter import StatsReporter

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/nimbusio_data_writer_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_data_writer_address = os.environ.get(
    "NIMBUSIO_DATA_WRITER_ADDRESS",
    "tcp://127.0.0.1:8100"
)
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
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

    state["writer"].start_new_segment(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    )

    state["writer"].store_sequence(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["segment-size"],
        b64decode(message["segment-md5-digest"]),
        message["segment-adler32"],
        sequence_num,
        data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(data))

    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].finish_new_segment(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        _extract_meta(message),
        message["segment-num"],
        message.get("conjoined_id"),
        message.get("conjoined_num"),
        message.get("conjoined_complete"),
        message["file-size"],
        message["file-adler32"],
        b64decode(message["file-hash"]),
        file_tombstone=False,
        handoff_node_id=handoff_node_id
    )

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

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

    state["writer"].start_new_segment(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    )

    state["writer"].store_sequence(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["segment-size"],
        b64decode(message["segment-md5-digest"]),
        message["segment-adler32"],
        message["sequence-num"],
        data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(data))

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

    state["writer"].store_sequence(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["segment-size"],
        b64decode(message["segment-md5-digest"]),
        message["segment-adler32"],
        message["sequence-num"],
        data
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(data))

    reply = {
        "message-type"  : "archive-key-next-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_final(state, message, data):
    log = logging.getLogger("_handle_archive_key_final")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    state["writer"].store_sequence(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["segment-size"],
        b64decode(message["segment-md5-digest"]),
        message["segment-adler32"],
        message["sequence-num"],
        data
    )

    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].finish_new_segment(
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        _extract_meta(message),
        message["segment-num"],
        message.get("conjoined_id"),
        message.get("conjoined_num"),
        message.get("conjoined_complete"),
        message["file-size"],
        message["file-adler32"],
        b64decode(message["file-hash"]),
        file_tombstone=False,
        handoff_node_id=handoff_node_id
    )

    Statgrabber.accumulate('nimbusio_write_requests', 1)
    Statgrabber.accumulate('nimbusio_write_bytes', len(data))

    reply = {
        "message-type"  : "archive-key-final-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_destroy_key(state, message, _data):
    log = logging.getLogger("_handle_destroy_key")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].set_tombstone(
        message["collection-id"], 
        message["key"], 
        timestamp,
        message["segment-num"]
    )

    reply = {
        "message-type"  : "destroy-key-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_purge_key(state, message, _data):
    log = logging.getLogger("_handle_purge_key")
    log.info("%s %s %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].purge_segment(
        message["collection-id"], 
        message["key"], 
        timestamp,
        message["segment-num"]
    )

    reply = {
        "message-type"  : "purge-key-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_start_conjoined_archive(state, message, _data):
    log = logging.getLogger("_handle_start_conjoined_archive")
    log.info("%r %r %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["conjoined-identifier-hex"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

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
        message["conjoined-identifier-hex"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

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
        message["conjoined-identifier-hex"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    reply = {
        "message-type"  : "finish-conjoined-archive-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

def _handle_delete_conjoined_archive(state, message, _data):
    log = logging.getLogger("_handle_delete_conjoined_archive")
    log.info("%r %r %s %s" % (
        message["collection-id"], 
        message["key"], 
        message["conjoined-identifier-hex"],
        message["timestamp-repr"],
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    reply = {
        "message-type"  : "delete-conjoined-archive-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : "success",
        "error-message" : None,
    }
    state["resilient-server"].send_reply(reply)

_dispatch_table = {
    "archive-key-entire"        : _handle_archive_key_entire,
    "archive-key-start"         : _handle_archive_key_start,
    "archive-key-next"          : _handle_archive_key_next,
    "archive-key-final"         : _handle_archive_key_final,
    "destroy-key"               : _handle_destroy_key,
    "purge-key"                 : _handle_purge_key,
    "start-conjoined-archive"   : _handle_start_conjoined_archive,
    "abort-conjoined-archive"   : _handle_abort_conjoined_archive,
    "finish-conjoined-archive"  : _handle_finish_conjoined_archive,
    "delete-conjoined-archive"  : _handle_delete_conjoined_archive,
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
    state["writer"] = Writer(
        state["database-connection"],
        _repository_path
    )

    state["stats-reporter"] = StatsReporter(state)

    state["event-push-client"].info("program-start", "data_writer starts")  

    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["stats-reporter"].run, state["stats-reporter"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping resilient server")
    state["resilient-server"].close()
    state["event-push-client"].close()

    state["zmq-context"].term()

    state["writer"].close()
    state["database-connection"].close()

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


