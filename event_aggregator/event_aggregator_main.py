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

from tools.router_server import RouterServer
from tools.sub_client import SUBClient
from tools.zeromq_pollster import ZeroMQPollster
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process
from tools.event_push_client import EventPushClient, \
        exception_event, \
        unhandled_exception_topic

_log_path = u"%s/nimbusio_event_aggregator.log" % (
    os.environ["NIMBUSIO_LOG_DIR"],
)
_event_publisher_pub_addresses = \
    os.environ["NIMBUSIO_EVENT_PUBLISHER_PUB_ADDRESSES"].split()
_event_aggregator_address = os.environ["NIMBUSIO_EVENT_AGGREGATOR_ADDRESS"]
_event_aggregator_topics = os.environ.get("NIMBUSIO_EVENT_AGGREGATOR_TOPICS",
    unhandled_exception_topic,
).split()

def _handle_subscription(state, message, _data):
    """we assume any unknown message type is a subscription"""
    log = logging.getLogger("_handle_subscription")
    log.info(str(message))

def _handle_critical_event_request(state, message, _data):
    """
    report any critical events
    """
    log = logging.getLogger("_handle_request_event_status")

_dispatch_table = {
    "critical-event-request"    :  _handle_critical_event_request,
}

def _create_state():
    return {
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "router-server"             : None,
        "event-push-client"         : None,
        "sub-clients"               : None,
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
        "cluster-row"               : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    state["router-server"] = RouterServer(
        state["zmq-context"],
        _event_aggregator_address, 
        state["receive-queue"]
    )
    state["router-server"].register(state["pollster"])

    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "event_aggregator"
    )

    state["sub-clients"] = list()
    for event_publisher_pub_address in _event_publisher_pub_addresses:
        sub_client = SUBClient(
            state["zmq-context"],
            event_publisher_pub_address,
            _event_aggregator_topics,
            state["receive-queue"]
        )
        sub_client.register(state["pollster"])
        state["sub-clients"].append(sub_client)

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table,
        default_handler=_handle_subscription
    )

    state["event-push-client"].info("program-start", "event_aggregator starts")

    timer_driven_callbacks = [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
    ] 
    return timer_driven_callbacks

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping server")
    state["router-server"].close()

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

