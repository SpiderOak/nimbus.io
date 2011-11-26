# -*- coding: utf-8 
"""
stats_subscriber.py

subscribe to the event aggregator for stats
"""
from collections import deque
import logging
import os
import sys
import time

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.sub_client import SUBClient
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process

_log_path = u"%s/stats_subscriber.log" % (os.environ["NIMBUSIO_LOG_DIR"], )
_event_aggregator_pub_address = \
        os.environ["NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS"]
_web_server_line_template = """
%(node-name)-20s %(source)-20s %(message-type)s %(count)3d
""".strip()
_queue_size_line_template = """
%(node-name)-20s %(source)-20s %(message-type)s %(queue_size)5d
""".strip()
_sub_topics = [
    "web-server-active-archives", 
    "web-server-active-retrieves", 
    "data-writer-receive-queue-size",
    "data-reader-receive-queue-size",
]

def _handle_web_server_stats(state, message, _data):
    log = logging.getLogger("_handle_web_server_stats")

    report_line = _web_server_line_template % message
    log.info(report_line)
    print report_line

def _handle_queue_size(state, message, _data):
    log = logging.getLogger("_handle_queue_size")

    report_line = _queue_size_line_template % message
    log.info(report_line)
    print report_line

_dispatch_table = {
    "web-server-active-archives"        : _handle_web_server_stats, 
    "web-server-active-retrieves"       : _handle_web_server_stats, 
    "data-writer-receive-queue-size"    : _handle_queue_size,
    "data-reader-receive-queue-size"    : _handle_queue_size,
}

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

    log.info("connecting sub-client to %s" % (_event_aggregator_pub_address, ))
    state["sub-client"] = SUBClient(
        state["zmq-context"],
        _event_aggregator_pub_address,
        _sub_topics,
        state["receive-queue"]
    )
    state["sub-client"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
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

