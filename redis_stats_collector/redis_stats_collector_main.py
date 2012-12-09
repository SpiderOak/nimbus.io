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

_collection_ops_accounting_insert = """
    insert into nimbusio_central.collection_ops_accounting( 
            collection_id,
            node_id,
            timestamp,
            duration,
            retrieve_request,
            retrieve_success,
            retrieve_error,
            archive_request,
            archive_success,
            archive_error,
            listmatch_request,
            listmatch_success,
            listmatch_error,
            delete_request,
            delete_success,
            delete_error,
            socket_bytes_in,
            socket_bytes_out,
            success_bytes_in,
            success_bytes_out,
            error_bytes_in,
            error_bytes_out)
   values (
            %(collection_id)s,
            %(node_id)s,
            %(timestamp)s,
            %(duration)s,
            %(retrieve_request)s,
            %(retrieve_success)s,
            %(retrieve_error)s,
            %(archive_request)s,
            %(archive_success)s,
            %(archive_error)s,
            %(listmatch_request)s,
            %(listmatch_success)s,
            %(listmatch_error)s,
            %(delete_request)s,
            %(delete_success)s,
            %(delete_error)s,
            %(socket_bytes_in)s,
            %(socket_bytes_out)s,
            %(success_bytes_in)s,
            %(success_bytes_out)s,
            %(error_bytes_in)s,
            %(error_bytes_out)s
          );"""

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

def _retrieve_node_dict(central_db_connection):
    rows = central_db_connection.fetch_all_rows("""
        select name, id from nimbusio_central.node
        where cluster_id = (select cluster_id from nimbusio_central.node
                            where name = %s)""", [_local_node_name, ])
    return dict(rows)

def _retrieve_dedupe_set(central_db_connection, node_id):
    rows = central_db_connection.fetch_all_rows("""
        select redis_key
        from nimbusio_central.collection_ops_accounting_flush_dedupe
        where node_id = %s""", [node_id, ])
    return set([redis_key.decode("utf-8") for (redis_key, ) in rows])

def _process_one_node(node_name,
                      node_id,
                      timestamp_cutoff,
                      dedupe_set,
                      collection_ops_accounting_rows,
                      new_dedupes,
                      node_keys_processed):
    log = logging.getLogger("_process_one_node")
    redis_connection = create_redis_connection(host=node_name)
    keys = redis_connection.keys("{0}.*".format(_key_prefix))
    log.debug("found {0} keys from {1}".format(len(keys), node_name))
    
    value_dict = dict()

    for key_bytes in keys:
        key = key_bytes.decode("utf-8")

        # nimbus.io.collection_ops_accounting.2012.12.04.22.12.archive_success
        key_as_list = key.split(".")
        timestamp_list = key_as_list[3:8]
        timestamp = datetime(int(timestamp_list[0]),
                             int(timestamp_list[1]),
                             int(timestamp_list[2]),
                             hour=int(timestamp_list[3]),
                             minute=int(timestamp_list[4]))
        partial_key = key_as_list[8]

        if timestamp > timestamp_cutoff:
            log.debug("ignoring recent key {0}".format(key))
            continue

        node_keys_processed.append(key)
        if key in dedupe_set:
            log.debug("ignoring duplicate key {0}".format(key))
            continue

        log.info("node = {0}, key = {1}".format(node_name, key))

        hash_dict = redis_connection.hgetall(key)
        for collection_id_bytes, count_bytes in hash_dict.items():
            collection_id = int(collection_id_bytes)
            count = int(count_bytes)

            log.info("    collection_id = {0}, count = {1}".format(
                collection_id, count))

            value_key = (timestamp, collection_id, )
            if not value_key in value_dict:
                value_dict[value_key] = \
                    _collection_ops_accounting_row(node_id, 
                                                   collection_id, 
                                                   timestamp)
            
            value_dict[value_key][partial_key] += count
        new_dedupes.append((node_id, key, ))

    collection_ops_accounting_rows.extend(value_dict.values())

def _insert_accounting_rows(central_db_connection, 
                            collection_ops_accounting_rows):
    for row in collection_ops_accounting_rows:
        central_db_connection.execute(_collection_ops_accounting_insert,
                                      row)

def _insert_dedupe_rows(central_db_connection, timestamp_cutoff, new_dedupes):
    for node_id, redis_key in new_dedupes:
        central_db_connection.execute("""
            insert into nimbusio_central.collection_ops_accounting_flush_dedupe
            values (node_id, redis_key, timestamp)
            (%s, %s, %s:timestamp)""", [node_id, redis_key, timestamp_cutoff])

def _remove_processed_keys(node_name, keys_processed):
    log = logging.getLogger("_remove_processed_keys")
    redis_connection = create_redis_connection(host=node_name)
    for key in keys_processed:
        log.info("removing {0}".format(key))
        redis_connection.delete(key)

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

    # don't flush anything newer than 1 minute ago
    current_time = datetime.utcnow()
    timestamp_cutoff = current_time - timedelta(minutes=1)

    return_code = 0
    central_db_connection = None

    collection_ops_accounting_rows = list()

    # values to be added to the dedupe table
    new_dedupes = list()

    # keys to be deleted (a list for each node
    node_keys_processed = [list() for _ in _node_names]

    try:
        central_db_connection = get_central_connection()

        # On startup, the program connects to the central database and tries 
        # to acquire a pg_advisory_lock appropriate for this program and the 
        # data center it is running in using the pg_try_advisory_lock function.
        # If it cannot acquire the lock, it notes the status of the lock 
        # and exits. This central locking mechanism lets us avoid single points
        # of failure by configuring the program to run on multiple nodes.

        with advisory_lock(central_db_connection, "redis_stats_collector"):
            node_dict = _retrieve_node_dict(central_db_connection)
            for node_name, keys_processed in \
                zip(_node_names, node_keys_processed):
                node_id = node_dict[node_name]
                log.debug("processing node {0} node_id={1}".format(node_name,
                                                                  node_id))

                # The program then selects into memory all recently collected 
                # keys from the central database table 
                # collection_ops_accounting_flush_dedupe and stores them in a 
                # dedupe set. This set allows runs of the collection/flush 
                # program to be idempotent across some time period (
                # but we won't keep the list of old keys forever.) 

                dedupe_set = _retrieve_dedupe_set(central_db_connection, 
                                                  node_id)

                # The program then visits the Redis instance on every storage 
                # node in the local data center, collecting the data from all 
                # past stats keys -- aggregating it into the program's memory.  
                # The aggregation should involve buckets for each 
                # storage_node_id and redis key, corresponding to the columns 
                # in the database.
                _process_one_node(node_name,
                                  node_dict[node_name],
                                  timestamp_cutoff,
                                  dedupe_set,
                                  collection_ops_accounting_rows,
                                  new_dedupes,
                                  keys_processed)

            # After collecting past keys from every storage node, 
            # inside a central database transaction:
            # 1. Insert the collected stats into the central database 
            #    collection_ops_accounting
            # 2. Insert collected keys into recently collected keys 
            #    collection_ops_accounting_flush_dedupe.
            # 3. commit transaction
            log.debug("updating central database")
            central_db_connection.begin_transaction()
            try:
                _insert_accounting_rows(central_db_connection,
                                        collection_ops_accounting_rows)
                _insert_dedupe_rows(central_db_connection, 
                                    timestamp_cutoff, 
                                    new_dedupes)
            except Exception:
                central_db_connection.rollback()
                raise
            else:
                central_db_connection.commit()

            # Then revisit the Redis nodes, and delete the keys we flushed 
            # into the database, and any keys we skipped because they were 
            # found in the dedupe set.
            for node_name, keys_processed in zip(_node_names, 
                                                 node_keys_processed):
                _remove_processed_keys(node_name, keys_processed)

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

