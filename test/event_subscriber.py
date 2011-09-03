# -*- coding: utf-8 -*-
"""
event_subscriber.py

subscribe to one node's event_publisher
"""
from collections import deque
import logging
import os
import sys
import time

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.sub_client import SUBClient
from diyapi_tools.callback_dispatcher import CallbackDispatcher
from diyapi_tools import time_queue_driven_process

_log_path = u"%s/event_subscriber.log" % (os.environ["NIMBUSIO_LOG_DIR"], )
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_event_publisher_pub_address = \
        os.environ["NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESS"]

def _handle_incoming_message(state, message, _data):
    log = logging.getLogger("_handle_incoming_message")
    log.debug(str(message))
    print str(message)

def _create_state():
    return {
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
        "sub-client"                : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    log.info("connecting sub-client to %s" % (_event_publisher_pub_address, ))
    state["sub-client"] = SUBClient(
        state["zmq-context"],
        _event_publisher_pub_address,
        "",
        state["receive-queue"]
    )
    state["sub-client"].register(state["pollster"])

    state["queue-dispatcher"] = CallbackDispatcher(
        state,
        state["receive-queue"],
        _handle_incoming_message
    )

    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping sub client")
    state["sub-client"].close()

    state["zmq-context"].term()

    log.debug("teardown complete")

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ],
        )
    )

