# -*- coding: utf-8 -*-
"""
diyapi_handoff_server_main.py

"""
from base64 import b64encode
from collections import deque, namedtuple
import logging
import os
import os.path
import sys
import time

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.resilient_server import ResilientServer
from diyapi_tools.resilient_client import ResilientClient
from diyapi_tools.pull_server import PULLServer
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process

from diyapi_handoff_server.hint_repository import HintRepository
from diyapi_handoff_server.data_writer_status_checker import \
        DataWriterStatusChecker
from diyapi_handoff_server.forwarder_coroutine import forwarder_coroutine

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_handoff_server_%s.log" % (
    _local_node_name,
)
_data_reader_addresses = \
    os.environ["DIYAPI_DATA_READER_ADDRESSES"].split()
_data_writer_addresses = \
    os.environ["DIYAPI_DATA_WRITER_ADDRESSES"].split()
_client_tag = "handoff_server-%s" % (_local_node_name, )
_handoff_server_address = os.environ.get(
    "DIYAPI_HANDOFF_SERVER_ADDRESS",
    "ipc:///tmp/spideroak-diyapi-handoff_server-%s/socket" % (
        _local_node_name,
    )
)
_handoff_server_pipeline_address = os.environ.get(
    "DIYAPI_HANDOFF_SERVER_PIPELINE_ADDRESS",
    "tcp://127.0.0.1:8700"
    )
)

_retrieve_timeout = 30 * 60.0

def _handle_hinted_handoff(state, message, _data):
    log = logging.getLogger("_handle_hinted_handoff")
    log.info("%s, %s %s, %s, %s, version_number %s, segment_number %s" % (
        message["dest-node-name"], 
        message["avatar-id"], 
        format_timestamp(message["timestamp"]), 
        message["action"]
        message["key"],  
        message["version-number"], 
        message["segment-number"],
    ))

    reply = {
        "message-type"  : "handoff-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    try:
        state["hint-repository"].store(
            message["dest-node-name"],
            message["avatar-id"],
            message["timestamp"],
            message["key"],
            message["version-number"],
            message["segment-number"],
            message["action"],
            message["server-node-names"]
        )
    except Exception, instance:
        log.exception(str(instance))
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
    else:
        reply["result"] = "success"

    state["resilient-server"].send_reply(reply)

def _handle_retrieve_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_start_reply")
    message = RetrieveKeyStartReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != RetrieveKeyStartReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_retrieve_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_next_reply")
    message = RetrieveKeyNextReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != RetrieveKeyNextReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_retrieve_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_final_reply")
    message = RetrieveKeyFinalReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != RetrieveKeyFinalReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_archive_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_start_reply")
    message = ArchiveKeyStartReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != ArchiveKeyStartReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_archive_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_next_reply")
    message = ArchiveKeyNextReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != ArchiveKeyNextReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_archive_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_final_reply")
    message = ArchiveKeyFinalReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != ArchiveKeyFinalReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_purge_key_reply(state, message_body):
    log = logging.getLogger("_handle_purge_key_reply")
    message = PurgeKeyReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != PurgeKeyReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))

    # the last thing the coroutine wants to give us is our hint
    hint = state[message.request_id].next()
    try:
        state["hint-repository"].purge_hint(hint)
    except Exception, instance:
        log.exception(instance)

    # all done
    del state[message.request_id]            
    return _check_for_handoffs(state, hint.exchange)

_dispatch_table = {
    "hinted-handoff"               : _handle_hinted_handoff,
    _retrieve_key_start_reply_routing_key   : _handle_retrieve_key_start_reply,
    _retrieve_key_next_reply_routing_key    : _handle_retrieve_key_next_reply,
    _retrieve_key_final_reply_routing_key   : _handle_retrieve_key_final_reply,
    _archive_key_start_reply_routing_key    : _handle_archive_key_start_reply,
    _archive_key_next_reply_routing_key     : _handle_archive_key_next_reply,
    _archive_key_final_reply_routing_key    : _handle_archive_key_final_reply,
    _purge_key_reply_routing_key            : _handle_purge_key_reply,
}

def _create_state():
    return {
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "resilient-server"          : None,
        "pull-server"               : None,
        "reader-clients"            : list(),
        "writer-clients"            : list(),
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
        "hint-repository"           : None,
        "data-writer-status-checker": None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")
    state["hint-repository"] = HintRepository()

    log.info("binding resilient-server to %s" % (_handoff_server_address, ))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _handoff_server_address,
        state["receive-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    log.info("binding pull-server to %s" % (_handoff_server_pipeline_address, ))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _handoff_server_pipeline_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])
    
    for data_reader_address in _data_reader_addresses:
        data_reader_client = ResilientClient(
            state["zmq-context"],
            data_reader_address,
            _client_tag,
            _handoff_server_pipeline_address
        )
        data_reader_client.register(state["pollster"])
        state["reader-clients"].append(data_reader_client)

    for data_writer_address in _data_writer_addresses:
        data_writer_client = ResilientClient(
            state["zmq-context"],
            data_writer_address,
            _client_tag,
            _handoff_server_pipeline_address
        )
        data_writer_client.register(state["pollster"])
        state["writer-clients"].append(data_writer_client)

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["data-writer-status-checker"] = DataWriterStatusChecker(state)

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["data-writer-status-checker"].run,
            DataWriterStatusChecker.next_run())
    ] 

def _tear_down(state):
    log = logging.getLogger("_tear_down")
    state["hint-repository"].close()
    del state["hint-repository"]

    log.debug("stopping resilient server")
    state["resilient-server"].close()

    log.debug("stopping pull server")
    state["pull-server"].close()

    log.debug("closing reader clients")
    for reader_client in state["reader-clients"]:
        reader_client.close()

    log.debug("closing writer clients")
    for writer_client in state["writer-clients"]:
        writer_client.close()

    state["zmq-context"].term()

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

