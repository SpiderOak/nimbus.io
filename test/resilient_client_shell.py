# -*- coding: utf-8 -*-
"""
resilient_client_shell.py

simple zeromq_driven_process running a ResilientClient
"""
from collections import deque
import logging
import os
import sys
import time
import uuid

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.pull_server import PULLServer
from diyapi_tools.resilient_client import ResilientClient
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/resilient_client_shell.log" % (
    os.environ["NIMBUSIO_LOG_DIR"],
)
_test_server_address = os.environ.get(
    "NIMBUSIO_TEST_SERVER_ADDRESS",
    "tcp://127.0.0.1:8000"
)
_test_client_address = os.environ.get(
    "NIMBUSIO_TEST_CLIENT_ADDRESS",
    "tcp://127.0.0.1:9000"
)

class EchoRequestSender(object):
    """
    send echo requests to the test server
    """
    interval = 1.0
    def __init__(self, state):
        self._state = state
        self._log = logging.getLogger("EchoRequestSender")
        self._sequence = 0

    def run(self, halt_event):
        if halt_event.is_set():
            self._log.info("halt_event is set")
            return []

        message = {
            "message-type"      : "echo-request",
            "request-id"        : uuid.uuid1().hex,
            "sequence"          : self._sequence,
        }
        self._state["test-client"].queue_message_for_send(message)

        self._sequence += 1
        
        return [(self.run, time.time() + self.interval, ), ] 

def _handle_echo_reply(state, message, _data):
    log = logging.getLogger("_handle_echo_reply")
    log.info("received %s" % (message, ))
    
    if message["sequence"] != state["expected-sequence"]:
        log.warn("sequence mismatch: message=%s; state=%s" % (
            message["sequence"], state["expected-sequence"],
        ))

    state["expected-sequence"] = message["sequence"] + 1

_dispatch_table = {
    "echo-reply"              : _handle_echo_reply,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "expected-sequence"     : 0,
        "pull-server"           : None,
        "test-client"           : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
    }

def _setup(_halt_event, _state):
    log = logging.getLogger("_setup")

    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _test_client_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    state["test-client"] = ResilientClient(
        state["zmq-context"],
        state["pollster"],
        _local_node_name,
        _test_server_address,
        _local_node_name,
        _test_client_address,
    )

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["sender"] = EchoRequestSender(state)

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["test-client"].run, 
         time.time() + ResilientClient.polling_interval),
        (state["sender"].run, time.time() + EchoRequestSender.interval, ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping test client")
    state["test-client"].close()
    state["pull-server"].close()

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

