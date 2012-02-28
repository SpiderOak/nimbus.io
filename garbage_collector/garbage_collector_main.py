# -*- coding: utf-8 -*-
"""
garbage_collector_main.py
"""
import io
import logging
import os
import sys

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection
from tools.event_push_client import EventPushClient
from tools.data_definitions import segment_status_active, \
        segment_status_cancelled, \
        segment_status_final, \
        segment_status_tombstone

from garbage_collector.options import get_options
from garbage_collector.versioned_collections import get_versioned_collections
from garbage_collector.candidate_partition_generator import \
        generate_candidate_partitions
from garbage_collector.archiver import archive_collectable_segment_rows

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_garbage_collector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

def _evaluate_partition(
    collectable_segment_ids, partition, versioned_collection
):
    """
    return a list of segment_id's that are candidates for collection

    Row Collection Decision Rules:

    * Tombstone rows are not collectable in this phase (but see below.)
    * status=Canceled rows are always collectable
    * status=Active rows are never collectable
    * A row is collectable if a later tombstone exists 
      where file_tombstone_unified_id is null
    * A row is collectable if a specific matching tombstone exists 
      (where file_tombstone_unified_id = unified_id)
    * If versioning is not enabled for the collection, 
      a row is collectable if a later version exists

    In the above, the word "later" means "a higher numbered unified_id"
    """
    log = logging.getLogger("_evaluate_partition")
    collectable_count = 0

    # status=Active rows are never collectable
    # Tombstone rows are not collectable in this phase
    never_collectable = set([segment_status_active, segment_status_tombstone])

    # reverse the partition list to get the newest (highest unified_id) first
    partition.reverse()

    collect_the_rest = False
    collect_unified_ids = set()
    latest_version_unified_id = None

    log.debug("{0} {1} versioned={2}".format(
        partition[0].collection_id, 
        partition[0].key, 
        versioned_collection,
    ))

    for entry in partition:

        log.debug("    {0} {1} {2} {3} {4}".format(
            entry.status, 
            entry.unified_id,
            entry.file_tombstone_unified_id,
            entry.key_row_number,
            entry.key_row_count
        ))

        # A row is collectable if a later tombstone exists 
        # where file_tombstone_unified_id is null
        if collect_the_rest:
            if not entry.status in never_collectable:
                collectable_segment_ids.write("{0}\n".format(entry.segment_id))
                collectable_count += 1
            continue
        if entry.status == segment_status_tombstone \
        and entry.file_tombstone_unified_id is None:
            collect_the_rest = True
            continue

        # A row is collectable if a specific matching tombstone exists 
        # (where file_tombstone_unified_id = unified_id)
        if entry.status == segment_status_tombstone:
            collect_unified_ids.add(entry.file_tombstone_unified_id)
            continue
        if entry.unified_id in collect_unified_ids:
            collectable_segment_ids.write("{0}\n".format(entry.segment_id))
            collectable_count += 1
            continue

        # status=Canceled rows are always collectable
        if entry.status == segment_status_cancelled:
            collectable_segment_ids.write("{0}\n".format(entry.segment_id))
            continue

        # status=Active rows are never collectable
        # Tombstone rows are not collectable in this phase
        if entry.status in never_collectable:
            continue

        # If versioning is not enabled for the collection, 
        # a row is collectable if a later version exists
        if not versioned_collection and entry.status == segment_status_final:
            if latest_version_unified_id is None:
                latest_version_unified_id = entry.unified_id
                continue
            # if we make it here, we could assume that we have
            # an older version, but let's check anyway
            if entry.unified_id < latest_version_unified_id:
                collectable_segment_ids.write("{0}\n".format(entry.segment_id))
                collectable_count += 1
                continue

    return collectable_count

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
        log.exception("Exception connecting to database {0}".format(instance))
        return -1

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "garbage_collector")
    event_push_client.info("program-start", "garbage_collector starts")  

    return_code = 0

    collectable_segment_ids = io.StringIO()

    partition_count = 0
    collectable_count = 0

    try:
        versioned_collections = get_versioned_collections()
        for partition in generate_candidate_partitions(connection):
            partition_count += 1
            versioned_collection = \
                    partition[0].collection_id in versioned_collections
            count = _evaluate_partition(collectable_segment_ids, 
                                        partition,
                                        versioned_collection)
            collectable_count += count
        archive_collectable_segment_rows(connection, 
                                         collectable_segment_ids,
                                         options.max_node_offline_time)
        collectable_segment_ids.close()
    except Exception:
        log.exception("_garbage_collection")
        return_code = -2
    else:
        log.info(
            "found {0:,} candidates, collected {1:,} segments".format(
                partition_count, collectable_count
            )
        )
        log.info("program terminates normally")

    connection.close()

    event_push_client.close()
    zmq_context.term()

    return return_code

if __name__ == "__main__":
    sys.exit(main())
