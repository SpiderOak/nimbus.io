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
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.data_definitions import incoming_slice_size, \
        zfec_slice_size, \
        damaged_segment_defective_sequence, \
        damaged_segment_missing_sequence

from node_inspector.work_generator import make_batch_key, generate_work

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_node_inspector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

def _store_damaged_segment(connection, entry, status, sequence_numbers):
    connection.execute("""
        insert into nimbusio_node.damaged_segment (
            collection_id,
            key,
            status,
            unified_id, 
            timestamp,
            segment_num,
            conjoined_part,
            sequence_numbers
        ) values (
            %(collection_id)s,
            %(key)s,
            %(status)s,
            %(unified_id)s, 
            %(timestamp)s,
            %(segment_num)s,
            %(conjoined_part)s,
            %(sequence_numbers)s
        )
    """, {"collection_id"   : entry.collection_id,
          "key"             : entry.key,
          "status"          : status,
          "unified_id"      : entry.unified_id, 
          "timestamp"       : entry.timestamp,
          "segment_num"     : entry.segment_num,
          "conjoined_part"  : entry.conjoined_part,
          "sequence_numbers": sequence_numbers,
    })
    connection.commit()

def _process_work_batch(connection, batch):
    log = logging.getLogger("_process_work_batch")

    assert len(batch) > 0
    batch_key = make_batch_key(batch[0])
    log.info("batch {0}".format(batch_key))

    # we divide incoming files into N slices, 
    # each one gets a segment_sequence row
    # so we expect this batch to have N entries,
    expected_slice_count = batch[0].file_size // incoming_slice_size
    if batch[0].file_size % incoming_slice_size != 0:
        expected_slice_count += 1

    expected_sequence_numbers = set(range(0, expected_slice_count+1))
    actual_sequence_numbers = set([entry.sequence_num for entry in batch])
    missing_sequence_numbers = \
            list(expected_sequence_numbers - actual_sequence_numbers)

    if len(missing_sequence_numbers) > 0:
        missing_sequence_numbers.sort()
        log.info("missing sequence numbers {0}".format(
            missing_sequence_numbers))
        _store_damaged_segment(connection, 
                               batch[0], 
                               damaged_segment_missing_sequence,
                               missing_sequence_numbers)

    # the size of data we should have for the file on this node
    expected_file_size = \
            zfec_slice_size(batch[-1].file_size + batch[-1].zfec_padding_size)
    sum_of_sequence_sizes = sum([entry.sequence_size for entry in batch])
    if sum_of_sequence_sizes != expected_file_size:
        log.info("node_file_size={0}; computed_file_size={1} {2}".format(
            node_file_size, computed_file_size, 
            [(entry.sequence_size, entry.zfec_padding_size) for entry in batch]
        ))

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    try:
        connection = get_node_local_connection()
    except Exception as instance:
        exctype, value = sys.exc_info()[:2]
        log.exception("Exception connecting to database {0}".format(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return -1

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "node_inspector")
    event_push_client.info("program-start", "node_inspector starts")  

    try:
        for batch in generate_work(connection):
            _process_work_batch(connection, batch)
    except Exception as instance:
        exctype, value = sys.exc_info()[:2]
        log.exception("Exception processing batch {0} {1}".format(
            batch, instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return -1

    connection.close()

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

