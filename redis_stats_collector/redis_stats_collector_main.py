#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
flush_stats_from_redis_main.py
See Ticket #65 Collect and Flush Stats from Redis on Storage Nodes to Central DB
"""
from datetime, timedelta
import logging
import os
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.process_util import set_signal_handler
from tools.database_connection import get_central_connection
from tools.advisory_lock import advisory_lock

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_redis_stats_collector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)

def _collection_ops_accounting_row(node_id, collection_id, timestamp):
    """
    corresponds to one row in table nimbusio_central.collection_ops_accounting
    """
    return {"collection_id" : collection_id,
            "node_id"       : node_id,
            "timestamp"     : timestamp,
            "duration"      : "1 minute",
            "retrieve_request" : 0,
            "retrieve_success" : 0,
            "retrieve_error" : 0,
            "archive_request" : 0,
            "archive_success" : 0,
            "archive_error" : 0,
            "listmatch_request" : 0,
            "listmatch_success" : 0,
            "listmatch_error" : 0,
            "delete_request" : 0,
            "delete_success" : 0,
            "delete_error" : 0,
            "socket_bytes_in" : 0,
            "socket_bytes_out" : 0,
            "success_bytes_in" : 0,
            "success_bytes_out" : 0,
            "error_bytes_in" : 0,
            "error_bytes_out" : 0,}

def _retrieve_dedupe_set(connection):
    rows = connection.fetch_all_rows("""
        select redis_key, node_id
        from nimbusio_central.collection_ops_accounting_flush_dedupe""")
    return set(rows)

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context =  zmq.Context()

    event_push_client = EventPushClient(zeromq_context, 
                                        "redis_stats_collector")
    event_push_client.info("program-start", "flush_stats_from_redis starts")  

    current_time = datetime.utcnow()
    flush_time = current_time - timedelta(minutes=1)

    return_code = 0
    connection = None

    # values to be added to the dedupe table
    new_dedupes = list()

    # keys to be deleted (a list for each node
    node_keys_processed = [list() for _ in _node_names]

    try:
        connection = get_central_connection()
        with advisory_lock(connection, "redis_stats_collector"):
            dedupe_set = _retrieve_dedupe_set(connection)
            for node_name, keys_processed in \
                zip(_node_names, node_keys_processed):
                _process_one_node(connection, node_name,

    except Exception as instance:
        log.exception("Uhandled exception {0}".format(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return_code = 1

    if connection is not None:
        connection.close()

    event_push_client.close()
    zeromq_context.term()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())

