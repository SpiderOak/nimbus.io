#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
flush_stats_from_redis_main.py
See Ticket #65 Collect and Flush Stats from Redis on Storage Nodes to Central DB
"""
from datetime import datetime, timedelta
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
from tools.redis_connection import create_redis_connection

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_redis_stats_collector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_key_prefix = "nimbus.io.collection_ops_accounting"

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

def _retrieve_dedupe_set(central_db_connection, node_name):
    rows = central_db_connection.fetch_all_rows("""
        select redis_key
        from nimbusio_central.collection_ops_accounting_flush_dedupe
        where node_id = (select id from nimbusio_central.node
                         where name = %s)""", [node_name, ])
    return set([redis_key for (redis_key, ) in rows])

def _process_one_node(central_db_connection, 
                      node_name,
                      dedupe_set,
                      collection_ops_accounting_rows,
                      new_dedupes,
                      node_keys_processed):
    log = logging.getLogger("_process_one_node")
    redis_connection = create_redis_connection(host=node_name)
    keys = redis_connection.keys("{0}.*".format(_key_prefix))
    log.debug("found {0} keys from {1}".format(len(keys), node_name))
    for key_bytes in keys:
        key = key_bytes.decode("utf-8")
        log.info("key = {0}".format(key))

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
    central_db_connection = None

    collection_ops_accounting_rows = list()

    # values to be added to the dedupe table
    new_dedupes = list()

    # keys to be deleted (a list for each node
    node_keys_processed = [list() for _ in _node_names]

    try:
        central_db_connection = get_central_connection()
        with advisory_lock(central_db_connection, "redis_stats_collector"):
            for node_name, keys_processed in \
                zip(_node_names, node_keys_processed):

                dedupe_set = _retrieve_dedupe_set(central_db_connection, 
                                                  node_name)

                _process_one_node(central_db_connection, 
                                  node_name,
                                  dedupe_set,
                                  collection_ops_accounting_rows,
                                  new_dedupes,
                                  node_keys_processed)

    except Exception as instance:
        log.exception("Uhandled exception {0}".format(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return_code = 1

    if central_db_connection is not None:
        central_db_connection.close()

    event_push_client.close()
    zeromq_context.term()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())

