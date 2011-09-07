# -*- coding: utf-8 -*-
"""
resilient_server_shell.py

simple zeromq_driven_process running a ResilientServer
"""
from collections import deque
import logging
import os
import sys
import time

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/resilient_server_shell.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], 
)
_test_server_address = os.environ.get(
    "NIMBUSIO_TEST_SERVER_ADDRESS",
    "tcp://127.0.0.1:8000"
)

def _handle_echo_request(state, message, _data):
    log = logging.getLogger("_handle_echo_request")
    log.info("received %s" % (message, ))
    
    message["message-type"]  = "echo-reply"

    state["test-server"].send_reply(message)

_dispatch_table = {
    "echo-request"              : _handle_echo_request,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "test-server"           : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
    }

def _setup(_halt_event, _state):
    log = logging.getLogger("_setup")

    state["test-server"] = ResilientServer(
        state["zmq-context"],
        _test_server_address,
        state["receive-queue"]
    )
    state["test-server"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping test server")
    state["test-server"].close()

    state["zmq-context"].term()
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

