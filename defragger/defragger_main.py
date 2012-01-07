# -*- coding: utf-8 -*-
"""
defragger.py
"""
from collections import namedtuple
import logging
import os
import signal
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.data_definitions import value_file_template

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_defragger_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_defrag_check_interval = 300.0
_database_retry_interval = 300.0
_min_bytes_for_defrag_pass = int(
    os.environ.get("NIMBUSIO_MIN_BYTES_FOR_DEFRAG_PASS", "3000000000")
)
_max_bytes_for_defrag_pass = int(
    os.environ.get("NIMBUSIO_MAX_BYTES_FOR_DEFRAG_PASS", "10000000000")
)
_reference_template = namedtuple("Reference", [
    "segment_id",
    "handoff_node_id",
    "collection_id", 
    "key", 
    "timestamp",
    "sequence_num",
    "value_file_id",
    "value_file_offset",
    "sequence_size",
    "sequence_hash",
    "sequence_adler32"
])

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _query_value_file_candidate_rows(local_connection):
    """
    query the database for qualifying value files:

    * closed
    * have more than 1 distinct collection, or null distinct collection
      (likely b/c of unclean shutdown)
    """
    result = local_connection.generate_all_rows("""
        select {0} from nimbusio_node.value_file 
        where close_time is not null
        and (distinct_collection_count > 1 or 
             distinct_collection_count is null)
        order by creation_time
    """.format(",".join(value_file_template._fields), ), [])
    for row in result:
        yield  value_file_template._make(row)

def _identify_defrag_candidates(local_connection):
    log = logging.getLogger("_identify_defrag_candidates")
    candidates = list()
    defragable_bytes = 0
    for value_file_row in _query_value_file_candidate_rows(local_connection):
       if defragable_bytes > _min_bytes_for_defrag_pass and \
          defragable_bytes + value_file_row.size > _max_bytes_for_defrag_pass:
           break
       candidates.append(value_file_row)
       defragable_bytes += value_file_row.size

    if defragable_bytes < _min_bytes_for_defrag_pass:
        log.debug("too few defragable bytes {0} in {1} value files".format(
            defragable_bytes, len(candidates)
        ))
        return 0, []

    log.debug("found {0} defragable bytes in {1} value files".format(
        defragable_bytes, len(candidates)
    ))
    return defragable_bytes, candidates

def _query_value_file_references(local_connection, value_file_ids):
    """
    Query the database to obtain all references to those value files sorted by: 
    
     * segment.handoff_node_id, 
     * segment.collection_id, 
     * segment.key, 
     * segment.timestamp, 
     * segment_sequence.sequence_num
    """
    result = local_connection.generate_all_rows("""
        select segment.id,
               segment.handoff_node_id,
               segment.collection_id, 
               segment.key, 
               segment.timestamp,
               segment_sequence.sequence_num,
               segment_sequence.value_file_id,
               segment_sequence.value_file_offset,
               segment_sequence.size,
               segment_sequence.hash,
               segment_sequence.adler32
        from nimbusio_node.segment as segment 
        inner join nimbusio_node.segment_sequence as segment_sequence 
        on segment.id = segment_sequence.segment_id
        where segment_sequence.value_file_id in %s
        order by segment.handoff_node_id asc,
                 segment.collection_id asc,
                 segment.key asc,
                 segment.timestamp asc,
                 segment_sequence.sequence_num asc
    """, [tuple(value_file_ids), ]) 
    for row in result:
        yield  _reference_template._make(row)

def _defrag_pass(local_connection, event_push_client):
    """
    Make a single defrag pass
    Open target value files as follows:

     * one distinct value file per handoff node, regardless of collection_id
     * one distinct value file per collection_id

    return the number of bytes defragged
    """
    log = logging.getLogger("_defrag_pass")

    defragable_bytes, value_file_rows = _identify_defrag_candidates(
        local_connection
    )
    if defragable_bytes == 0:
        return 0

    prev_handoff_node_id = None
    prev_collection_id = None
    for reference in _query_value_file_references(
        local_connection, [row.id for row in value_file_rows]
    ):
        if reference.handoff_node_id is not None:
            # one distinct value file per handoff node
            if reference.handoff_node_id != prev_handoff_node_id:
                if prev_handoff_node_id is not None:
                    log.debug(
                        "closing value file for handoff node {0}".format(
                            prev_handoff_node_id
                        )
                    )
                log.debug(
                    "opening value file for handoff node {0}".format(
                        handoff_node_id
                    )
                )
                prev_handoff_node_id = reference.handoff_node_id
        elif reference.collection_id != prev_collection_id:
            if prev_handoff_node_id is not None:
                log.debug(
                    "closing value file for handoff node {0}".format(
                        prev_handoff_node_id
                    )
                )
                prev_handoff_node_id = None

            # one distinct value file per collection_id
            if prev_collection_id is not None:
                log.debug(
                    "closing value file for collection {0}".format(
                        prev_collection_id
                    )
                )
            log.debug(
                "opening value file for collection {0}".format( 
                    reference.collection_id
                )
            )
            prev_collection_id = reference.collection_id

    if prev_handoff_node_id is not None:
        log.debug(
            "closing value file for handoff node {0}".format(
                prev_handoff_node_id
            )
        )

    if prev_collection_id is not None:
        log.debug(
            "closing value file for collection {0}".format(
                prev_collection_id
            )
        )

    return 0

def main():
    """
    main entry point

    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "defragger")
    event_push_client.info("program-start", "defragger starts")  

    local_connection = None

    while not halt_event.is_set():

        # if we don't have an open database connection, get one
        if local_connection is None:
            try:
                local_connection = get_node_local_connection()
            except Exception as instance:
                exctype, value = sys.exc_info()[:2]
                event_push_client.exception(
                    "database exception",
                    str(value),
                    exctype=exctype.__name__
                )
                log.exception("Exception connecting to database")
                halt_event.wait(_database_retry_interval)
                continue

        # try one defrag pass
        bytes_defragged = 0
        try:
            bytes_defragged = _defrag_pass(
                local_connection, event_push_client
            )
        except KeyboardInterrupt:
            halt_event.set()
        except Exception as instance:
            log.exception(str(instance))
            event_push_client.exception(
                unhandled_exception_topic,
                str(instance),
                exctype=instance.__class__.__name__
            )

        log.info("bytes defragged = {0}".format(bytes_defragged))

        # if we didn't do anythibng on this pass...
        if bytes_defragged == 0:

            # close the database connection
            if local_connection is not None:
                local_connection.close()
                local_connection = None

            # wait and try again
            try:
                halt_event.wait(_defrag_check_interval)
            except KeyboardInterrupt:
                halt_event.set()
                
    if local_connection is not None:
        local_connection.close()

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

