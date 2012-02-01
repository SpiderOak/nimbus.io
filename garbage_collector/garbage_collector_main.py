# -*- coding: utf-8 -*-
"""
garbage_collector_main.py
"""
import io
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
from garbage_collector.archiver import archive_collectable_segment_rows

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_garbage_collector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

def _evaluate_versioned_partition(collectable_segment_ids, partition):
    """
    return a list of segment_id's that are candidates for collection

    * Tombstone rows are not collectable in this phase (but see below.)
    * A row is collectable if a later tombstone exists where 
      file_tombstone_unified_id is null
    * A row is collectable if a specific matching tombstone exists 
      (where file_tombstone_unified_id = unified_id)
    """
    # reverse the partition list to get the newest (highest unified_id) first
    partition.reverse()

    collect_the_rest = False
    collect_unified_ids = set()
    for entry in partition:
        if collect_the_rest:
            if not entry.file_tombstone:
                collectable_segment_ids.write("{0}\n".format(entry.segment_id))
            continue
        if entry.file_tombstone and entry.file_tombstone_unified_id is None:
            collect_the_rest = True
            continue
        if entry.file_tombstone:
            collect_unified_ids.add(entry.file_tombstone_unified_id)
            continue
        if entry.unified_id in collect_unified_ids:
            collectable_segment_ids.write("{0}\n".format(entry.segment_id))
            continue

def _evaluate_unversioned_partition(collectable_segment_ids, partition):
    """
    return a list of segment_id's that are candidates for collection

    * If versioning is not enabled for the collection, 
      a row is collectable if a later version exists
    """
    # reverse the partition list to get the newest (highest unified_id) first
    partition.reverse()

    collect_the_rest = False
    for entry in partition:
        if collect_the_rest:
            if not entry.file_tombstone:
                collectable_segment_ids.write("{0}\n".format(entry.segment_id))
            continue
        # this must be the most recent entry, (i.e. highest unified_id)
        # we want to collect every segment that comes before it 
        collect_the_rest = True

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

    collectable_segment_ids = io.StringIO()

    try:
        versioned_collections = get_versioned_collections()
        for partition in generate_candidate_partitions(connection):
            if partition[0].collection_id in versioned_collections:
                _evaluate_versioned_partition(
                    collectable_segment_ids, partition
                )
            else:
                _evaluate_unversioned_partition(
                    collectable_segment_ids, partition
                )
        archive_collectable_segment_rows(
            connection, collectable_segment_ids
        )
        collectable_segment_ids.close()
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
