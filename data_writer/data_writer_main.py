#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_writer_main.py

message handling for data writer
"""
import logging
import os
import queue
import sys
from threading import Event

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.rep_server import REPServer
from tools.standard_logging import initialize_logging
from tools.sub_client import SUBClient
from tools.push_client import PUSHClient
from tools.event_push_client import EventPushClient
from tools.database_connection import get_central_connection
from tools.process_util import set_signal_handler

from web_public_reader.central_database_util import get_cluster_row, \
        get_node_rows

from data_writer.reply_pull_server import ReplyPULLServer
from data_writer.writer_thread import WriterThread
from data_writer.sync_thread import SyncThread

class AppendQueue(queue.Queue):
    """
    adapter to give a Queue an 'append' member.
    """
    def append(self, item):
        self.put(item)

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_data_writer_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_data_writer_address = os.environ["NIMBUSIO_DATA_WRITER_ADDRESS"]
_data_writer_anti_entropy_address = \
        os.environ["NIMBUSIO_DATA_WRITER_ANTI_ENTROPY_ADDRESS"]
_event_aggregator_pub_address = \
        os.environ["NIMBUSIO_EVENT_AGGREGATOR_PUB_ADDRESS"]
_writer_thread_reply_address = "inproc://writer_thread_reply"

def _create_state():
    return {
        "halt-event"            : Event(),
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "resilient-server"      : None,
        "reply-pull-server"     : None,
        "anti-entropy-server"   : None,
        "sub-client"            : None,
        "event-push-client"     : None,
        "message-queue"         : AppendQueue(),
        "cluster-row"           : None,
        "node-rows"             : None,
        "node-id-dict"          : None,
        "writer-thread"         : None,
        "sync-thread"           : None,
    }

def _setup(state):
    log = logging.getLogger("_setup")

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "data_writer"
    )

    log.info("binding resilient-server to {0}".format(_data_writer_address))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _data_writer_address,
        state["message-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    log.info("binding reply-pull-server to {0}".format(
        _writer_thread_reply_address))
    state["reply-pull-server"] = ReplyPULLServer(
        state["zmq-context"],
        _writer_thread_reply_address,
        state["resilient-server"].send_reply
    )
    state["reply-pull-server"].register(state["pollster"])

    log.info("binding anti-entropy-server to {0}".format(
        _data_writer_anti_entropy_address))
    state["anti-entropy-server"] = REPServer(
        state["zmq-context"],
        _data_writer_anti_entropy_address,
        state["message-queue"]
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
        state["message-queue"]
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

    state["event-push-client"].info("program-start", "data_writer starts")


    state["reply-push-client"] = PUSHClient(state["zmq-context"],
                                            _writer_thread_reply_address)

    state["writer-thread"] = WriterThread(state["halt-event"],
                                          state["node-id-dict"],
                                          state["message-queue"],
                                          state["reply-push-client"])
    state["writer-thread"].start()

    state["sync-thread"] = SyncThread(state["halt-event"],
                                      state["message-queue"])
    state["sync-thread"].start()

def _tear_down(state):
    log = logging.getLogger("_tear_down")

    log.debug("joining writer thread")
    state["writer-thread"].join(timeout=3.0)

    log.debug("joining sync thread")
    state["sync-thread"].join(timeout=3.0)

    log.debug("stopping resilient server")
    state["resilient-server"].close()
    state["reply-pull-server"].close()
    state["anti-entropy-server"].close()
    state["sub-client"].close()
    state["event-push-client"].close()
    state["reply-push-client"].close()

    state["zmq-context"].term()

    log.debug("teardown complete")

def main():
    """
    main entry point
    """
    returncode = 0
    
    initialize_logging(_log_path)

    log = logging.getLogger("main")
    state = _create_state()
    set_signal_handler(state["halt-event"])

    try:
        _setup(state)
    except Exception:
        instance = sys.exc_info()[1]
        log.exception("unhandled exception in _setup")
        log.critical("unhandled exception in _setup {0}".format(
            instance))
        state["halt-event"].set()
        returncode = 1

    log.debug("start halt_event loop")
    while not state["halt-event"].is_set():
        try:
            state["pollster"].run(state["halt-event"])
        except Exception:
            instance = sys.exc_info()[1]
            log.exception("unhandled exception in pollster")
            log.critical("unhandled exception in pollster {0}".format(
                instance))
            state["halt-event"].set()
            returncode = 1
    log.debug("end halt_event loop")

    try:
        _tear_down(state)
    except Exception:
        instance = sys.exc_info()[1]
        log.exception("unhandled exception in _tear_down")
        log.critical("unhandled exception in _tear_down {0}".format(
            instance))
        returncode = 1

    return returncode

if __name__ == "__main__":
    sys.exit(main())
