# -*- coding: utf-8 -*-
"""
event_publisher.py

recieve events from all processes on a node wiht PULLServer
publish events with PUBServer
"""
from collections import deque
import logging
import os
import sys
import time

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.pub_server import PUBServer
from tools.pull_server import PULLServer
from tools.callback_dispatcher import CallbackDispatcher
from tools import time_queue_driven_process

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/nimbusio_event_publisher_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

_event_publisher_pull_address = \
        os.environ["NIMBUSIO_EVENT_PUBLISHER_PULL_ADDRESS"]
_event_publisher_pub_address = \
        os.environ["NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESS"]

def _handle_incoming_message(state, message, _data):
    log = logging.getLogger("_handle_incoming_message")
    log.debug(str(message))
    message["node-name"] = _local_node_name
    state["pub-server"].send_message(message)

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "pull-server"           : None,
        "pub-server"            : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    log.info("binding pub-server to %s" % (_event_publisher_pub_address, ))
    state["pub-server"] = PUBServer(
        state["zmq-context"],
        _event_publisher_pub_address,
    )

    log.info("binding pull-server to %s" % (_event_publisher_pull_address, ))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _event_publisher_pull_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    state["queue-dispatcher"] = CallbackDispatcher(
        state,
        state["receive-queue"],
        _handle_incoming_message
    )

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping pub server")
    state["pub-server"].close()

    log.debug("stopping pull server")
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

