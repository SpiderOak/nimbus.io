# -*- coding: utf-8 -*-
"""
diyapi_data_writer_main.py

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
from collections import deque
import logging
import os
import sys
import time

import zmq

import Statgrabber

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.resilient_server import ResilientServer
from diyapi_tools.event_push_client import EventPushClient, exception_event
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.pandora_database_connection import get_node_local_connection
from diyapi_tools.data_definitions import parse_timestamp_repr
from diyapi_web_server.database_util import node_rows

from diyapi_data_writer.writer import Writer

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_data_writer_%s.log" % (
    _local_node_name,
)
_data_writer_address = os.environ.get(
    "DIYAPI_DATA_WRITER_ADDRESS",
    "tcp://127.0.0.1:8100"
)
_repository_path = os.environ.get(
    "DIYAPI_REPOSITORY_PATH", os.environ.get("PANDORA_REPOSITORY_PATH")
)

def _handle_archive_key_entire(state, message, data):
    log = logging.getLogger("_handle_archive_key_entire")
    log.info("%s %s %s %s" % (
        message["avatar-id"], 
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
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    )

    state["writer"].store_sequence(
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        sequence_num,
        data
    )

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].finish_new_segment(
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["file-size"],
        message["file-adler32"],
        b64decode(message["file-hash"]),
        message["file-user-id"],
        message["file-group-id"],
        message["file-permissions"],
        file_tombstone=False,
        handoff_node_id=handoff_node_id
    )

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_start(state, message, data):
    log = logging.getLogger("_handle_archive_key_start")
    log.info("%s %s %s %s" % (
        message["avatar-id"], 
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
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    )

    state["writer"].store_sequence(
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["sequence-num"],
        data
    )

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_next(state, message, data):
    log = logging.getLogger("_handle_archive_key_next")
    log.info("%s %s %s %s" % (
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    state["writer"].store_sequence(
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["sequence-num"],
        data
    )

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

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
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    state["writer"].store_sequence(
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["sequence-num"],
        data
    )

    if message["handoff-node-name"] is None:
        handoff_node_id = None
    else:
        handoff_node_id = state["node-id-dict"][message["handoff-node-name"]]

    state["writer"].finish_new_segment(
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"],
        message["file-size"],
        message["file-adler32"],
        b64decode(message["file-hash"]),
        message["file-user-id"],
        message["file-group-id"],
        message["file-permissions"],
        file_tombstone=False,
        handoff_node_id=handoff_node_id
    )

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

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
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].set_tombstone(
        message["avatar-id"], 
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
        message["avatar-id"], 
        message["key"], 
        message["timestamp-repr"],
        message["segment-num"]
    ))

    timestamp = parse_timestamp_repr(message["timestamp-repr"])

    state["writer"].purge_segment(
        message["avatar-id"], 
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

_dispatch_table = {
    "archive-key-entire"    : _handle_archive_key_entire,
    "archive-key-start"     : _handle_archive_key_start,
    "archive-key-next"      : _handle_archive_key_next,
    "archive-key-final"     : _handle_archive_key_final,
    "destroy-key"           : _handle_destroy_key,
    "purge-key"             : _handle_purge_key,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "resilient-server"      : None,
        "event-push-client"     : None,
        "state-cleaner"         : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
        "writer"                : None,
        "database-connection"   : None,
        "node-rows"             : None,
        "node-id-dict"          : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    log.info("binding resilient-server to %s" % (_data_writer_address, ))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _data_writer_address,
        state["receive-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "data_writer"
    )

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["database-connection"] = get_node_local_connection()
    state["node-rows"] = node_rows(state["database-connection"])
    state["node-id-dict"] = dict(
        [(node_row.name, node_row.id, ) for node_row in state["node-rows"]]
    )

    state["writer"] = Writer(
        state["database-connection"],
        _repository_path
    )

    state["event-push-client"].info("program-start", "data_writer starts")  

    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
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


