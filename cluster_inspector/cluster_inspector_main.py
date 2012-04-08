# -*- coding: utf-8 -*-
"""
cluster_inspector_main.py

"""
from collections import namedtuple, deque
import logging
import os
import os.path
import cPickle as pickle
import random
import sys
from threading import Event
import time

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.event_push_client import EventPushClient, exception_event
from tools.resilient_client import ResilientClient
from tools.pull_server import PULLServer
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process
from tools.database_connection import \
        get_node_local_connection, \
        get_central_connection
from tools.data_definitions import segment_row_template, \
        conjoined_row_template, \
        create_priority
from tools.LRUCache import LRUCache

from web_server.central_database_util import get_cluster_row, \
        get_node_rows

from cluster_inspector.segment_puller import SegmentPuller

class ClusterInspectorError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_inspector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_data_reader_addresses = \
    os.environ["NIMBUSIO_DATA_READER_ADDRESSES"].split()
_data_writer_addresses = \
    os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"].split()
_client_tag = "cluster_inspector-{0}".format(_local_node_name)
_cluster_inspector_pipeline_address = os.environ.get(
    "NIMBUSIO_CLUSTER_INSPECTOR_PIPELINE_ADDRESS",
    "tcp://127.0.0.1:8115"
)

def _handle_cluster_inspector_segments_reply(state, message, _data):
    log = logging.getLogger("_handle_cluster_inspector_segments_reply")
    log.info("node {0}".format(message["node-name"])) 

_dispatch_table = {
    "cluster-inspector-segments-reply" : \
        _handle_cluster_inspector_segments_reply,
}

def _create_state():
    return {
        "halt-event"                : Event(),
        "database-connection"       : None,
        "cluster-row"               : None,
        "node-rows"                 : None,
        "node-id-dict"              : None,
        "node-name-dict"            : None,
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "event-push-client"         : None,
        "pull-server"               : None,
        "reader-client-dict"        : dict(),
        "writer-client-dict"        : dict(),
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")
    status_checkers = list()

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "cluster_inspector"
    )

    central_connection = get_central_connection()
    state["cluster-row"] = get_cluster_row(central_connection)
    state["node-rows"] = get_node_rows(
        central_connection, state["cluster-row"].id
    )
    central_connection.close()

    state["node-id-dict"] = dict(
        [(node_row.name, node_row.id, ) for node_row in state["node-rows"]]
    )
    state["node-name-dict"] = dict(
        [(node_row.id, node_row.name, ) for node_row in state["node-rows"]]
    )

    state["database-connection"] = get_node_local_connection()

    log.info("binding pull-server to {0}".format(
        _cluster_inspector_pipeline_address))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _cluster_inspector_pipeline_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    for node_row, data_reader_address in zip(
        state["node-rows"], _data_reader_addresses
    ):
        data_reader_client = ResilientClient(
            state["zmq-context"],
            state["pollster"],
            node_row.name,
            data_reader_address,
            _client_tag,
            _cluster_inspector_pipeline_address
        )
        state["reader-client-dict"][data_reader_client.server_node_name] = \
                data_reader_client
        # don't run all the status checkers at the same time
        status_checkers.append(
            (data_reader_client.run, time.time() + random.random() * 60.0, )
        )        

    for node_row, data_writer_address in zip(
        state["node-rows"], _data_writer_addresses
    ):
        data_writer_client = ResilientClient(
            state["zmq-context"],
            state["pollster"],
            node_row.name,
            data_writer_address,
            _client_tag,
            _cluster_inspector_pipeline_address
        )
        state["writer-client-dict"][data_writer_client.server_node_name] = \
                data_writer_client
        # don't run all the status checkers at the same time
        status_checkers.append(
            (data_writer_client.run, time.time() + random.random() * 60.0, )
        )        

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    segment_puller = SegmentPuller(state)

    state["event-push-client"].info("program-start", "handoff_server starts")  

    timer_driven_callbacks = [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (segment_puller.start_pullers, time.time(), ),
    ] 
    timer_driven_callbacks.extend(status_checkers)
    return timer_driven_callbacks

def _tear_down(state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping pull server")
    state["pull-server"].close()

    log.debug("closing reader clients")
    for reader_client in state["reader-client-dict"].values():
        reader_client.close()

    log.debug("closing writer clients")
    for writer_client in state["writer-client-dict"].values():
        writer_client.close()

    state["event-push-client"].close()

    state["zmq-context"].term()

    state["database-connection"].close()

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ],
            exception_action=exception_event,
            halt_event=state["halt-event"]
        )
    )

