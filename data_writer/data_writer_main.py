#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_writer_main.py

message handling for data writer
"""
from base64 import b64decode
import hashlib
import logging
import os
import sys
import time

import zmq

import Statgrabber

from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.rep_server import REPServer
from tools.sub_client import SUBClient
from tools.event_push_client import EventPushClient, exception_event
from tools.priority_queue import PriorityQueue
from tools.deque_dispatcher import DequeDispatcher
from tools.database_connection import get_central_connection
from tools.data_definitions import parse_timestamp_repr

from web_public_reader.central_database_util import get_cluster_row, \
        get_node_rows

from data_writer.writer_thread import writer_thread_reply_address, WriterThread

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_data_writer_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_data_writer_address = os.environ["NIMBUSIO_DATA_WRITER_ADDRESS"]
_data_writer_anti_entropy_address = \
        os.environ["NIMBUSIO_DATA_WRITER_ANTI_ENTROPY_ADDRESS"]
_event_aggregator_pub_address = \
        os.environ["NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS"]

def _create_state():
    return {
        "halt-event"            : Event()
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "resilient-server"      : None,
        "anti-entropy-server"   : None,
        "sub-client"            : None,
        "event-push-client"     : None,
        "receive-queue"         : PriorityQueue(),
        "cluster-row"           : None,
        "node-rows"             : None,
        "node-id-dict"          : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "data_writer"
    )

    log.info("binding resilient-server to %{0}".format(_data_writer_address))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _data_writer_address,
        state["receive-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    log.info("binding anti-entropy-server to {0}".format(
        _data_writer_anti_entropy_address))
    state["anti-entropy-server"] = REPServer(
        state["zmq-context"],
        _data_writer_anti_entropy_address,
        state["receive-queue"]
    )
    state["anti-entropy-server"].register(state["pollster"])

    topics = ["web-writer-start", ]
    log.info("connecting sub-client to {0} subscribing to {1}".format(
        _event_aggregator_pub_address,
        topics))
    state["sub-client"] = SUBClient(
        state["zmq-context"],
        _event_aggregator_pub_address,
        topics,
        state["receive-queue"],
        queue_action="prepend"
    )
    state["sub-client"].register(state["pollster"])

    central_connection = get_central_connection()
    state["cluster-row"] = get_cluster_row(central_connection)
    state["node-rows"] = get_node_rows(
        central_connection, state["cluster-row"].id
    )
    central_connection.close()

    state["node-id-dict"] = dict(
        [(node_row.name, node_row.id, ) for node_row in state["node-rows"]]
    )

    state["sync-manager"] = SyncManager(state["writer"])

    state["event-push-client"].info("program-start", "data_writer starts")

    return [
        (state["pollster"].run, time.time(), ),
        (state["queue-dispatcher"].run, time.time(), ),
        (state["stats-reporter"].run, state["stats-reporter"].next_run(), ),
        (state["sync-manager"].run, state["sync-manager"].next_run(), ),
    ]

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping writer thread")
    state["writer-thread"].join(timeout=3.0)

    log.debug("stopping resilient server")
    state["resilient-server"].close()
    state["anti-entropy-server"].close()
    state["sub-client"].close()
    state["event-push-client"].close()

    state["zmq-context"].term()

    log.debug("teardown complete")

def main():
    """
    main entry point
    """
    state = _create_state()
    _setup()


    _teardown()

if __name__ == "__main__":
    sys.exit(main())
