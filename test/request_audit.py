# -*- coding: utf-8 -*-
"""
request_audit.py

request anti entroy audit of an collection
arguments <collection-id>
"""
from collections import deque
import logging
import os
import sys
from threading import Event
import time

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.pull_server import PULLServer
from tools.resilient_client import ResilientClient
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process

_log_path = u"%s/request_audit.log" % (s.environ["NIMBUSIO_LOG_DIR"], )
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_client_tag = "request-audit-%s" % (_local_node_name, )
_anti_entropy_server_address = \
    os.environ["NIMBUSIO_ANTI_ENTROPY_SERVER_ADDRESS"]
_pipeline_address = "tcp://127.0.0.1:6666"

def _handle_anti_entropy_audit_reply(state, message, _data):
    log = logging.getLogger("_handle_anti_entropy_audit_reply")

    if message["result"] == "success":
        info = "audit OK!"
        log.info(info)
        print >> sys.stderr, info
    else:
        info = "error %s" % (message["error-message"], )
        log.error(info)
        print >> sys.stderr, info

    state["halt-event"].set()

def _create_state():
    return {
        "halt-event"                : Event(),
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
        "pull-server"               : None,
        "anti-entropy-client"       : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    collection_id = int(sys.argv[1])

    log.info("binding pull-server to %s" % (_pipeline_address, ))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _pipeline_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    log.info("connecting anti-entropy-client to %s" % (
        _anti_entropy_server_address, 
    ))
    state["anti-entropy-client"] = ResilientClient(
        state["zmq-context"],
        state["pollster"],
        _local_node_name,
        _anti_entropy_server_address,
        _client_tag,
        _pipeline_address
    )

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    info = "requesting audit of %s" % (collection_id, )
    log.info(info)
    print >> sys.stderr, ""
    print >> sys.stderr, info

    message = {
        "message-type"  : "anti-entropy-audit-request",
        "collection-id"     : collection_id,
    }
    state["anti-entropy-client"].queue_message_for_send(message)

    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["anti-entropy-client"].run, time.time(), ),
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping anti entropy client")
    state["pull-server"].close()
    state["anti-entropy-client"].close()

    state["zmq-context"].term()

    log.debug("teardown complete")

_dispatch_table = {
    "anti-entropy-audit-reply"    : _handle_anti_entropy_audit_reply,
}

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ],
            halt_event=state["halt-event"]
        )
    )

