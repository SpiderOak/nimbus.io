# -*- coding: utf-8 -*-
"""
event_aggegator_main.py

"""
from collections import deque
import logging
import os
import sys
import time

import zmq

from tools.pub_server import PUBServer
from tools.sub_client import SUBClient
from tools.zeromq_pollster import ZeroMQPollster
from tools.callback_dispatcher import CallbackDispatcher
from tools import time_queue_driven_process
from tools.event_push_client import EventPushClient, \
        exception_event

_log_path = u"%s/nimbusio_event_aggregator.log" % (
    os.environ["NIMBUSIO_LOG_DIR"],
)
_event_publisher_pub_addresses = \
    os.environ["NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESSES"].split()
_event_aggregator_pub_address = \
    os.environ["NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS"]
_subscribe_to_all_topics = ""

def _publish_event(state, message, _data):
    """
    re-publish an incoming event
    """
    state["pub-server"].send_message(message)

def _create_state():
    return {
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "pub-server"                : None,
        "event-push-client"         : None,
        "sub-clients"               : None,
        "receive-queue"             : deque(),
        "callback-dispatcher"       : None,
        "cluster-row"               : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")
    log.info("starting up")

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "event_aggregator"
    )

    state["pub-server"] = PUBServer(
        state["zmq-context"],
        _event_aggregator_pub_address 
    )

    state["sub-clients"] = list()
    for event_publisher_pub_address in _event_publisher_pub_addresses:
        sub_client = SUBClient(
            state["zmq-context"],
            event_publisher_pub_address,
            _subscribe_to_all_topics,
            state["receive-queue"]
        )
        sub_client.register(state["pollster"])
        state["sub-clients"].append(sub_client)

    state["callback-dispatcher"] = CallbackDispatcher(
        state,
        state["receive-queue"],
        _publish_event,
    )

    state["event-push-client"].info("program-start", "event_aggregator starts")

    timer_driven_callbacks = [
        (state["pollster"].run, time.time(), ), 
        (state["callback-dispatcher"].run, time.time(), ), 
    ] 
    return timer_driven_callbacks

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping server")
    state["pub-server"].close()

    log.debug("stopping sub clients")
    for sub_client in state["sub-clients"]:
        sub_client.close()
    state["event-push-client"].close()

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
            exception_action=exception_event
        )
    )

