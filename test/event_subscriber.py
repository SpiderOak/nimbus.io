# -*- coding: utf-8 
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

from tools.zeromq_pollster import ZeroMQPollster
from tools.sub_client import SUBClient
from tools.callback_dispatcher import CallbackDispatcher
from tools import time_queue_driven_process
from tools.event_push_client import level_cmp

_log_path = u"%s/event_subscriber.log" % (os.environ["NIMBUSIO_LOG_DIR"], )
_event_aggregator_pub_address = \
        os.environ["NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS"]
_report_line_template = "%20(node-name)s %20(source)s %(description)s"

def _handle_incoming_message(state, message, _data):
    log = logging.getLogger("_handle_incoming_message")
    if level_cmp(message["level"], state["min-level"]) < 0:
        return

    report_line = _report_line_template % message
    log.info(report_line)
    print report_line

def _create_state():
    return {
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "receive-queue"             : deque(),
        "callback-dispatcher"       : None,
        "sub-client"                : None,
        "min-level"                 : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    if len(sys.argv) < 2:
        state["min-level"] = "debug"
    else:
        state["min-level"] = sys.argv[1]

    # verify that we have a valid level
    assert level_cmp(state["min-level"], state["min-level"]) == 0
    log.info("minimum event level = %s" % (state["min-level"], ))

    log.info("connecting sub-client to %s" % (_event_aggregator_pub_address, ))
    state["sub-client"] = SUBClient(
        state["zmq-context"],
        _event_aggregator_pub_address,
        "",
        state["receive-queue"]
    )
    state["sub-client"].register(state["pollster"])

    state["callback-dispatcher"] = CallbackDispatcher(
        state,
        state["receive-queue"],
        _handle_incoming_message
    )

    return [
        (state["pollster"].run, time.time(), ), 
        (state["callback-dispatcher"].run, time.time(), ), 
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

