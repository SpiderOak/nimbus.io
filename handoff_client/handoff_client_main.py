#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
handoff_client_main.py
"""
import logging
import os
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_connection
from tools.event_push_client import EventPushClient, unhandled_exception_topic

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_handoff_client_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,)
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_node_database_hosts = \
    os.environ["NIMBUSIO_NODE_DATABASE_HOSTS"].split()
_node_database_ports = \
    os.environ["NIMBUSIO_NODE_DATABASE_PORTS"].split()
_node_database_passwords = \
    os.environ["NIMBUSIO_NODE_USER_PASSWORDS"].split() 

def _retrieve_conjoined_handoffs_for_node(interaction_pool, node_id):
    query = """
        select * from nimbusio_node.conjoined
        where handoff_node_id = %s
        order by unified_id
    """
    async_result = \
        interaction_pool.run(interaction=query, 
                             interaction_args=[node_id, ],
                             pool=_local_node_name) 
    result_list = async_result.get()

    conjoined_row_list = list()
    for row in result_list:
        # bytea columns come out of the database as buffer objects
        if row["combined_hash"] is not None: 
            row["combined_hash"] = str(row["combined_hash"])
        # row is of type psycopg2.extras.RealDictRow
        # we want something we can pickle
        conjoined_row_list.append(dict(row.items()))

    return conjoined_row_list

def _retrieve_segment_handoffs_for_node(interaction_pool, node_id):
    query = """
        select * from nimbusio_node.segment 
        where handoff_node_id = %s
        order by timestamp desc
    """
    async_result = \
        interaction_pool.run(interaction=query, 
                             interaction_args=[node_id, ],
                             pool=_local_node_name) 
    result_list = async_result.get()

    segment_row_list = list()
    for row in result_list:
        # bytea columns come out of the database as buffer objects
        if row["file_hash"] is not None: 
            row["file_hash"] = str(row["file_hash"])
        # row is of type psycopg2.extras.RealDictRow
        # we want something we can pickle
        segment_row_list.append(dict(row.items()))

    return segment_row_list

def _get_handoff_segments():
    handoff_segments = list()
    return handoff_segments

def _process_handoff_segments(handoff_segments):
    pass

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "handoff_client")
    event_push_client.info("program-start", "handoff_client starts")  

    return_code = 0
    try:
        handoff_segments = _get_handoff_segments()
        if len(handoff_segments) > 0:
            _process_handoff_segments(handoff_segments)
    except Exception as instance:
        log.exception("Uhandled exception {0}".format(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return_code = 1

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())

