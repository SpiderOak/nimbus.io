# -*- coding: utf-8 -*-
"""
diyapi_handoff_server_main.py

"""
from collections import deque
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
from diyapi_tools.standard_logging import format_timestamp

from diyapi_handoff_server.hint_repository import HintRepository
from diyapi_handoff_server.data_writer_status_checker import \
        DataWriterStatusChecker

class HandoffError(Exception):
    pass

_node_names = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()
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

_retrieve_timeout = 30 * 60.0

def _handle_hinted_handoff(state, message, _data):
    log = logging.getLogger("_handle_hinted_handoff")
    log.info("%s, %s %s, %s, %s, version_number %s, segment_number %s" % (
        message["dest-node-name"], 
        message["avatar-id"], 
        format_timestamp(message["timestamp"]), 
        message["action"],
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

def _handle_retrieve_reply(state, message, data):
    log = logging.getLogger("_handle_retrieve_reply")

    try:
        forwarder = state["active-forwarders"].pop(message["message-id"])
    except KeyError:
        log.error("no forwarder for message %s" % (message, ))
        return

    #TODO: we need to squawk about this somehow
    if message["result"] != "success":
        error_message = "%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        )
        log.error(error_message)
        raise HandoffError(error_message)

    message_id = forwarder.send((message, data, ))
    assert message_id is not None
    state["active-forwarders"][message_id] = forwarder    

def _handle_archive_reply(state, message, _data):
    log = logging.getLogger("_handle_archive_reply")

    try:
        forwarder = state["active-forwarders"].pop(message["message-id"])
    except KeyError:
        log.error("no forwarder for message %s" % (message, ))
        return

    #TODO: we need to squawk about this somehow
    if message["result"] != "success":
        error_message = "%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        )
        log.error(error_message)
        raise HandoffError(error_message)

    message_id = forwarder.send(message)
    assert message_id is not None
    state["active-forwarders"][message_id] = forwarder    

def _handle_purge_key_reply(state, message, _data):
    log = logging.getLogger("_handle_purge_key_reply")

    try:
        forwarder = state["active-forwarders"].pop(message["message-id"])
    except KeyError:
        log.error("no forwarder for message %s" % (message, ))
        return

    #TODO: we need to squawk about this somehow
    if message["result"] != "success":
        log.error("%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        ))
        # we don't give up here, because the handoff has succeeded 
        # at this point we're just cleaning up

    # if we get back a string, it is a message-id for another purge
    # otherwise, we should get the hint we started with
    result = forwarder.send(message)
    assert result is not None

    if type(result) is str:
        message_id = result
        state["active-forwarders"][message_id] = forwarder
    else:
        hint = result
        log.info("handoff complete %s %s %s" % (
            hint.node_name, hint.avatar_id, hint.key
        ))
        try:
            state["hint-repository"].purge_hint(hint)
        except Exception, instance:
            log.exception(instance)
        state["data-writer-status-checker"].check_node_for_hint(hint.node_name)

_dispatch_table = {
    "hinted-handoff"                : _handle_hinted_handoff,
    "retrieve-key-start-reply"      : _handle_retrieve_reply,
    "retrieve-key-next-reply"       : _handle_retrieve_reply,
    "retrieve-key-final-reply"      : _handle_retrieve_reply,
    "archive-key-start-reply"       : _handle_archive_reply,
    "archive-key-next-reply"        : _handle_archive_reply,
    "archive-key-final-reply"       : _handle_archive_reply,
    "purge-key-reply"               : _handle_purge_key_reply,
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
        "active-forwarders"         : dict(),
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
    
    for node_name, data_reader_address in zip(
        _node_names, _data_reader_addresses
    ):
        data_reader_client = ResilientClient(
            state["zmq-context"],
            state["pollster"],
            node_name,
            data_reader_address,
            _client_tag,
            _handoff_server_pipeline_address
        )
        state["reader-clients"].append(data_reader_client)

    for node_name, data_writer_address in zip(
        _node_names, _data_writer_addresses
    ):
        data_writer_client = ResilientClient(
            state["zmq-context"],
            state["pollster"],
            node_name,
            data_writer_address,
            _client_tag,
            _handoff_server_pipeline_address
        )
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

