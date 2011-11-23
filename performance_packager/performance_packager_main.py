# -*- coding: utf-8 -*-
"""
performance_packager.py

subscribe to performance related events and package the results for reporting
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

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/nimbusio_performance_packager_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_event_aggregator_pub_address = \
        os.environ["NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS"]
_sub_topics = ["archive-stats", "retrieve-stats", ]
_report_template = "%s %-8s %8.02f min %6d bytes/sec"

def _handle_archive_stats(_state, message, _data):
    log = logging.getLogger("stats")
    elapsed_time = message["end_time"] - message["start_time"]
    if elapsed_time == 0:
        bytes_per_second = 0
    else:
        bytes_per_second = message["bytes_archived"] // elapsed_time

    log.info(_report_template % (
        message["node-name"], 
        "archive", 
        float(elapsed_time) / 60.0,
        bytes_per_second
    ))

def _handle_retrieve_stats(_state, message, _data):
    log = logging.getLogger("stats")
    elapsed_time = message["end_time"] - message["start_time"]
    if elapsed_time == 0:
        bytes_per_second = 0
    else:
        bytes_per_second = message["bytes_retrieved"] // elapsed_time

    log.info(_report_template % (
        message["node-name"], 
        "retrieve", 
        float(elapsed_time) / 60.0,
        bytes_per_second
    ))

_dispatch_table = {
    "archive-stats"     : _handle_archive_stats,
    "retrieve-stats"    : _handle_retrieve_stats,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "sub-client"            : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
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

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping sub client")
    state["sub-client"].close()

    log.debug("stopping zeromq context")
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

