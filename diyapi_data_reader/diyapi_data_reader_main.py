# -*- coding: utf-8 -*-
"""
diyapi_data_reader_main.py

Receives block read requests.
Looks up pointers to data by querying the database server
Looks for files in both the hashfanout area 
Responds with content or "not available"
"""
from collections import deque, namedtuple
import logging
import os.path
import sys
import time

import zmq

import Statgrabber

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.xrep_server import XREPServer
from diyapi_tools.xreq_client import XREQClient
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.persistent_state import load_state, save_state
from diyapi_tools import repository

from diyapi_database_server import database_content
from diyapi_data_reader.state_cleaner import StateCleaner

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_data_reader_%s.log" % (
    _local_node_name,
)
_persistent_state_file_name = "data-reader-%s" % (_local_node_name, )
_database_server_address = os.environ.get(
    "DIYAPI_DATABASE_SERVER_ADDRESS",
    "tcp://127.0.0.1:8000"
)
_data_reader_address = os.environ.get(
    "DIYAPI_DATA_READER_ADDRESS",
    "tcp://127.0.0.1:8200"
)
_key_lookup_timeout = 60.0
_retrieve_timeout = 30 * 60.0
_heartbeat_interval = float(
    os.environ.get("DIYAPI_DATA_READER_HEARTBEAT", "60.0")
)

_retrieve_state_tuple = namedtuple("RetrieveState", [ 
    "xrep_ident",
    "timeout",
    "timeout_message",
    "avatar_id",
    "key",
    "version_number",
    "segment_number",
    "segment_size",
    "sequence",
    "file_name",
])

def _handle_retrieve_key_start(state, message, _data):
    log = logging.getLogger("_handle_retrieve_key_start")
    log.info("avatar_id = %s, key = %s" % (
        message["avatar-id"], message["key"], 
    ))

    reply = {
        "message-type"  : "retrieve-key-start-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state["active-requests"]:
        error_string = "invalid duplicate request_id in RetrieveKeyStart"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _retrieve_state_tuple(
        xrep_ident=message["xrep-ident"],
        timeout=time.time()+_key_lookup_timeout,
        timeout_message="retrieve-key-start-reply",
        avatar_id = message["avatar-id"],
        key = message["key"],
        version_number=message["version-number"],
        segment_number=message["segment-number"],
        segment_size = None,
        sequence = None,
        file_name = None
    )

    # send a lookup request to the database, with the reply
    # coming back to us
    request = {
        "message-type"      : "key-lookup",
        "request-id"        : message["request-id"],
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : message["version-number"],
        "segment-number"    : message["segment-number"],
    }
    state["database-client"].queue_message_for_send(request)

def _handle_retrieve_key_next(state, message, _data):
    log = logging.getLogger("_handle_retrieve_key_next")

    try:
        retrieve_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    log.info("avatar_id = %s, key = %s sequence = %s" % (
        retrieve_state.avatar_id, retrieve_state.key, message["sequence"]
    ))

    reply = {
        "message-type"  : "retrieve-key-next-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    if retrieve_state.sequence is None \
    or message["sequence"] != retrieve_state.sequence+1:
        error_string = "%s %s out of sequence %s %s" % (
            retrieve_state.avatar_id, 
            retrieve_state.key,
            message["sequence"],
            retrieve_state.sequence+1
        )
        log.error(error_string)
        reply["result"] = "out-of-sequence"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    content_path = repository.content_path(
        retrieve_state.avatar_id, 
        retrieve_state.file_name
    ) 

    offset = message["sequence"] * retrieve_state.segment_size

    try:
        with open(content_path, "r") as input_file:
            input_file.seek(offset)
            data_content = input_file.read(retrieve_state.segment_size)
    except Exception, instance:
        log.exception("%s %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    state["active-requests"][message["request-id"]] = retrieve_state._replace(
        xrep_ident=None,
        timeout=time.time()+_retrieve_timeout,
        sequence=message["sequence"]
    )

    Statgrabber.accumulate('diy_read_requests', 1)
    Statgrabber.accumulate('diy_read_bytes', len(data_content))

    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply, data=data_content)

def _handle_retrieve_key_final(state, message, _data):
    log = logging.getLogger("_handle_retrieve_key_final")

    try:
        retrieve_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    log.info("avatar_id = %s, key = %s sequence = %s" % (
        retrieve_state.avatar_id, retrieve_state.key, message["sequence"]
    ))

    reply = {
        "message-type"  : "retrieve-key-final-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    if message["sequence"] != retrieve_state.sequence+1:
        error_string = "%s %s out of sequence %s %s" % (
            retrieve_state.avatar_id, 
            retrieve_state.key,
            message["sequence"],
            retrieve_state.sequence+1
        )
        log.error(error_string)
        reply["result"] = "out-of-sequence"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    content_path = repository.content_path(
        retrieve_state.avatar_id, 
        retrieve_state.file_name
    ) 

    offset = message["sequence"] * retrieve_state.segment_size

    try:
        with open(content_path, "r") as input_file:
            input_file.seek(offset)
            data_content = input_file.read(retrieve_state.segment_size)
    except Exception, instance:
        log.exception("%s %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    # we don't save the state, because we are done

    Statgrabber.accumulate('diy_read_requests', 1)
    Statgrabber.accumulate('diy_read_bytes', len(data_content))

    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply, data=data_content)

def _handle_key_lookup_reply(state, message, data):
    log = logging.getLogger("_handle_key_lookup_reply")

    try:
        retrieve_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply = {
        "message-type"  : "retrieve-key-start-reply",
        "xrep-ident"    : retrieve_state.xrep_ident,
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we got a database error, pass it on 
    if message["result"] != "success":
        log.error("%s %s database error: (%s) %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
            message["result"],
            message["error-message"],
        ))
        reply["result"] = "database-error"
        reply["error_message"] = message["error-message"]
        state["xrep-server"].queue_message_for_send(reply)
        return

    database_entry, _ = database_content.unmarshall(data, 0)

    # if this key is a tombstone, treat as an error
    if database_entry.is_tombstone:
        log.error("%s %s this record is a tombstone" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply["result"] = "no-such-key"
        reply["error_message"] = "is tombstone"
        state["xrep-server"].queue_message_for_send(reply)
        return

    content_path = repository.content_path(
        retrieve_state.avatar_id, 
        database_entry.file_name
    ) 

    try:
        with open(content_path, "r") as input_file:
            data_content = input_file.read(database_entry.segment_size)
    except Exception, instance:
        log.exception("%s %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    # if we have more than one segment, we need to save the state
    # otherwise this request is done
    if database_entry.segment_count > 1:
        state["active-requests"][message["request-id"]] = \
            retrieve_state._replace(
                timeout=time.time()+_retrieve_timeout,
                sequence=0, 
                segment_size=database_entry.segment_size,
                file_name=database_entry.file_name
            )

    Statgrabber.accumulate('diy_read_requests', 1)
    Statgrabber.accumulate('diy_read_bytes', len(data_content))

    reply["result"]             = "success"
    reply["timestamp"]          = database_entry.timestamp
    reply["is-tombstone"]       = database_entry.is_tombstone
    reply["version-number"]     = database_entry.version_number
    reply["segment-number"]     = database_entry.segment_number
    reply["segment-count"]      = database_entry.segment_count
    reply["segment-size"]       = database_entry.segment_size
    reply["total-size"]         = database_entry.total_size
    reply["file-adler32"]       = database_entry.file_adler32
    reply["file-md5"]           = database_entry.file_md5
    reply["segment-adler32"]    = database_entry.segment_adler32
    reply["segment-md5"]        = database_entry.segment_md5
    state["xrep-server"].queue_message_for_send(reply, data_content)

_dispatch_table = {
    "retrieve-key-start"    : _handle_retrieve_key_start,
    "retrieve-key-next"     : _handle_retrieve_key_next,
    "retrieve-key-final"    : _handle_retrieve_key_final,
    "key-lookup-reply"      : _handle_key_lookup_reply,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "xrep-server"           : None,
        "database-client"       : None,
        "state-cleaner"         : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
        "active-requests"       : dict(),
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    pickleable_state = load_state(_persistent_state_file_name)
    if pickleable_state is not None:
        for key, value in pickleable_state:
            state["active-requests"][key] = _retrieve_state_tuple(**value)

    log.info("binding xrep-server to %s" % (_data_reader_address, ))
    state["xrep-server"] = XREPServer(
        state["zmq-context"],
        _data_reader_address,
        state["receive-queue"]
    )
    state["xrep-server"].register(state["pollster"])

    state["database-client"] = XREQClient(
        state["zmq-context"],
        _database_server_address,
        state["receive-queue"]
    )
    state["database-client"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["state-cleaner"] = StateCleaner(state)

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["state-cleaner"].run, state["state-cleaner"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping xrep server")
    state["xrep-server"].close()

    log.debug("stopping database client")
    state["database-client"].close()

    state["zmq-context"].term()

    log.info("saving state")
    pickleable_state = dict()
    for request_id, request_state in state["active-requests"].items():
        pickleable_state[request_id] = request_state._asdict()

    save_state(pickleable_state, _persistent_state_file_name)
    log.debug("teardown complete")

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ]
        )
    )

