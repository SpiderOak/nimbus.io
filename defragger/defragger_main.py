# -*- coding: utf-8 -*-
"""
defragger.py
"""
from collections import namedtuple
import hashlib
import logging
import os
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.data_definitions import value_file_template
from tools.file_space import load_file_space_info, \
        file_space_sanity_check, \
        find_least_volume_space_id
from tools.output_value_file import OutputValueFile
from tools.process_util import set_signal_handler

from defragger.input_value_file import InputValueFile

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
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_max_value_file_size = int(os.environ.get(
    "NIMBUS_IO_MAX_VALUE_FILE_SIZE", str(1024 * 1024 * 1024))
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

def _query_value_file_candidate_rows(connection):
    """
    query the database for qualifying value files:

    * closed
    * have more than 1 distinct collection, or null distinct collection
      (likely b/c of unclean shutdown)
    """
    result = connection.generate_all_rows("""
        select {0} from nimbusio_node.value_file 
        where close_time is not null
        and (distinct_collection_count > 1 or 
             distinct_collection_count is null)
        order by creation_time
    """.format(",".join(value_file_template._fields), ), [])
    for row in result:
        yield  value_file_template._make(row)

def _identify_defrag_candidates(connection):
    log = logging.getLogger("_identify_defrag_candidates")
    candidates = list()
    defraggable_bytes = 0
    for value_file_row in _query_value_file_candidate_rows(connection):
        if defraggable_bytes > _min_bytes_for_defrag_pass and \
        value_file_row.size is not None and \
        defraggable_bytes + value_file_row.size > _max_bytes_for_defrag_pass:
            break
        candidates.append(value_file_row)
        if value_file_row.size is not None:
            defraggable_bytes += value_file_row.size

    if defraggable_bytes < _min_bytes_for_defrag_pass:
        log.info("too few defraggable bytes {0} in {1} value files".format(
            defraggable_bytes, len(candidates)
        ))
        return 0, []

    log.info("found {0:,} defraggable bytes in {1:,} value files".format(
        defraggable_bytes, len(candidates)
    ))
    return defraggable_bytes, candidates

def _query_value_file_references(connection, value_file_ids):
    """
    Query the database to obtain all references to those value files sorted by: 
    
     * segment.handoff_node_id, 
     * segment.collection_id, 
     * segment.key, 
     * segment.timestamp, 
     * segment_sequence.sequence_num
    """
    result = connection.generate_all_rows("""
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

def _generate_work(connection, file_space_info, value_file_rows):
    log = logging.getLogger("_generate_work")
    prev_handoff_node_id = None
    prev_collection_id = None
    output_value_file = None
    for reference in _query_value_file_references(
        connection, [row.id for row in value_file_rows]
    ):
        if reference.handoff_node_id is not None:
            # at least one distinct value file per handoff node
            if reference.handoff_node_id != prev_handoff_node_id:
                if prev_handoff_node_id is not None:
                    log.debug(
                        "closing output value file handoff node {0}".format(
                            prev_handoff_node_id
                        )
                    )
                    assert output_value_file is not None
                    output_value_file.close()
                    output_value_file = None
                log.debug(
                    "opening value file for handoff node {0}".format(
                        reference.handoff_node_id
                    )
                )
                assert output_value_file is None
                space_id = find_least_volume_space_id("storage", 
                                                      file_space_info)
                output_value_file = OutputValueFile(connection, 
                                                    space_id, 
                                                    _repository_path)
                prev_handoff_node_id = reference.handoff_node_id
        elif reference.collection_id != prev_collection_id:
            if prev_handoff_node_id is not None:
                log.debug(
                    "closing value file for handoff node {0}".format(
                        prev_handoff_node_id
                    )
                )
                assert output_value_file is not None
                output_value_file.close()
                output_value_file = None
                prev_handoff_node_id = None

            # at least one distinct value file per collection_id
            if prev_collection_id is not None:
                log.debug(
                    "closing value file for collection {0}".format(
                        prev_collection_id
                    )
                )
                assert output_value_file is not None
                output_value_file.close()
                output_value_file = None

            log.debug(
                "opening value file for collection {0}".format( 
                    reference.collection_id
                )
            )
            assert output_value_file is None
            space_id = find_least_volume_space_id("storage", 
                                                  file_space_info)
            output_value_file = OutputValueFile(
                connection, space_id, _repository_path
            )
            prev_collection_id = reference.collection_id

        assert output_value_file is not None

        # if this write would put us over the max size,
        # start a new output value file
        expected_size = output_value_file.size + reference.sequence_size
        if expected_size > _max_value_file_size:
            log.debug("closing value_file and opening new one due to size")
            output_value_file.close()
            space_id = find_least_volume_space_id("storage", 
                                                  file_space_info)
            output_value_file = OutputValueFile(
                connection, space_id, _repository_path
            )

        yield reference, output_value_file
    
    if prev_handoff_node_id is not None:
        log.debug(
            "closing final value file for handoff node {0}".format(
                prev_handoff_node_id
            )
        )

    if prev_collection_id is not None:
        log.debug(
            "closing final value file for collection {0}".format(
                prev_collection_id
            )
        )

    output_value_file.close()

def _defrag_pass(connection, file_space_info, event_push_client):
    """
    Make a single defrag pass
    Open target value files as follows:

     * one distinct value file per handoff node, regardless of collection_id
     * one distinct value file per collection_id

    return the number of bytes defragged
    """
    log = logging.getLogger("_defrag_pass")

    defraggable_bytes, value_file_rows = _identify_defrag_candidates(
        connection
    )
    if defraggable_bytes == 0:
        return 0

    input_value_files = dict()
    for value_file_row in value_file_rows:
        input_value_file = InputValueFile(
            connection, _repository_path, value_file_row
        )
        input_value_files[input_value_file.value_file_id] = input_value_file

    bytes_defragged = 0
    for reference, output_value_file in _generate_work(
        connection, file_space_info, value_file_rows
    ):
        # read the segment sequence from the old value_file
        input_value_file = input_value_files[reference.value_file_id]
        data = input_value_file.read(
            reference.value_file_offset, reference.sequence_size
        )
        sequence_md5 = hashlib.md5()
        sequence_md5.update(data)
        if sequence_md5.digest() != bytes(reference.sequence_hash):
            log.error(
                "md5 mismatch {0} {1} {2} {3} {4} {5} {6} {7} {8}".format(
                    reference.segment_id,
                    reference.handoff_node_id,
                    reference.collection_id, 
                    reference.key, 
                    reference.timestamp,
                    reference.sequence_num,
                    reference.value_file_id,
                    reference.value_file_offset,
                    reference.sequence_size
                )
            )
            #TODO - insert into repair table
            continue

        # write the segment_sequence to the new value file
        new_value_file_offset = output_value_file.size
        output_value_file.write_data_for_one_sequence(
            reference.collection_id, 
            reference.segment_id, 
            data
        )
        bytes_defragged += reference.sequence_size

        # adjust segment_sequence row
        connection.execute("""
            update nimbusio_node.segment_sequence
            set value_file_id = %s, value_file_offset = %s
            where collection_id = %s and segment_id = %s
            and sequence_num = %s
        """, [output_value_file.value_file_id, 
              new_value_file_offset,
              reference.collection_id,
              reference.segment_id,
              reference.sequence_num])

    # close (and remove) the old value files
    for input_value_file in input_value_files.values():
        input_value_file.close()

    return bytes_defragged

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

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "defragger")
    event_push_client.info("program-start", "defragger starts")  

    connection = None
    file_space_info = None

    while not halt_event.is_set():

        # if we don't have an open database connection, get one
        if connection is None:
            try:
                connection = get_node_local_connection()
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

            file_space_info = load_file_space_info(connection) 
            file_space_sanity_check(file_space_info, _repository_path)

        # start a transaction
        connection.execute("begin")

        # try one defrag pass
        bytes_defragged = 0
        try:
            bytes_defragged = _defrag_pass(connection, 
                                           file_space_info, 
                                           event_push_client)
        except KeyboardInterrupt:
            halt_event.set()
            connection.rollback()
        except Exception as instance:
            log.exception(str(instance))
            event_push_client.exception(
                unhandled_exception_topic,
                str(instance),
                exctype=instance.__class__.__name__
            )
            connection.rollback()
        else:
            connection.commit()

        log.info("bytes defragged = {0:,}".format(bytes_defragged))

        # if we didn't do anything on this pass...
        if bytes_defragged == 0:

            # close the database connection
            if connection is not None:
                connection.close()
                connection = None

            # wait and try again
            try:
                halt_event.wait(_defrag_check_interval)
            except KeyboardInterrupt:
                halt_event.set()
                
    if connection is not None:
        connection.close()

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

