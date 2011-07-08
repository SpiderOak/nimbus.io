# -*- coding: utf-8 -*-
"""
diyapi_event_publisher.py

recieve events from all processes on a node wiht PULLServer
publish events with PUBServer
"""
from collections import deque
import logging
import os
import sys
import time

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.pub_server import PUBServer
from diyapi_tools.pull_server import PULLServer
from diyapi_tools import time_queue_driven_process

from diyapi_event_publisher.callback_dispatcher import CallbackDispatcher

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_event_publisher_%s.log" % (
    _local_node_name,
)

_event_publisher_pull_address = \
        os.environ["DIYAPI_EVENT_PUBLISHER_PULL_ADDRESS"]
_event_publisher_pub_address = \
        os.environ["DIYAPI_EVENT_PUBLISHER_PUB_ADDRESS"]

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

