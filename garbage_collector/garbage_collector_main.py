# -*- coding: utf-8 -*-
"""
garbage_collector_main.py
"""
import logging
import os
import signal
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from garbage_collector.options import get_options
from garbage_collector.versioned_collections import get_versioned_collections
from garbage_collector.candidate_partition_generator import \
        generate_candidate_partitions

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_garbage_collector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

def _evaluate_one_partition(
    options, connection, event_push_client, versioned_collections, partition
):
    """
    * Tombstone rows are not collectable in this phase (but see below.)
    * A row is collectable if a later tombstone exists where 
      file_tombstone_unified_id is null
    * A row is collectable if a specific matching tombstone exists 
      (where file_tombstone_unified_id = unified_id)
    * If versioning is not enabled for the collection, 
      a row is collectable if a later version exists
    """
    log = logging.getLogger("_evaluate_on_partition")
    log.debug(str(partition))

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    options = get_options()

    try:
        connection = get_node_local_connection()
    except Exception as instance:
        log.exception("Exception connecting to database")
        return -1

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "garbage_collector")
    event_push_client.info("program-start", "garbage_collector starts")  

    return_code = 0

    try:
        versioned_collections = get_versioned_collections()
        for partition in generate_candidate_partitions(connection):
            _evaluate_one_partition(
                options, 
                connection, 
                event_push_client, 
                versioned_collections, 
                partition
            )
    except Exception:
        log.exception("_garbage_collection")
        return_code = -2
    else:
        log.info("program terminates normally")

    connection.close()

    event_push_client.close()
    zmq_context.term()

    return return_code

if __name__ == "__main__":
    sys.exit(main())
