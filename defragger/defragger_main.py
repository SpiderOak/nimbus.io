#!/usr/bin/env python3

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
        find_least_volume_space_id_and_path, \
        available_space_on_volume
from tools.output_value_file import OutputValueFile
from tools.process_util import set_signal_handler

from defragger.input_value_file import InputValueFile

class DefraggerInsufficentSpaceError(Exception):
    """
    Ticket #5866 Nimbus.io should avoid filling disks to capacity
    """
    pass

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
_min_destination_space = long(
    os.environ.get("NIMBUS_IO_DEFRAGGER_MIN_DESTINATION_SPACE",
        str(5 * 1024 ** 3)))

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

_work_template = namedtuple("Work", ["references", "value_file_size", ])

def _query_value_file_candidate_rows(connection):
    """
    query the database for qualifying value files:

    * closed
    * have more than 1 distinct collection, or null distinct collection
      (likely b/c of unclean shutdown)
    """
    result = connection.generate_all_rows("""
        select {0} from nimbusio_node.value_file 
        where 
        space_id=(select space_id 
                  from nimbusio_node.file_space 
                  where purpose='journal')
        and close_time is not null
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

def _prepare_work(connection, value_file_rows):
    """
    Ticket #5866 Nimbus.io should avoid filling disks to capacity
    Alan has added on the requirement to fallocate value files.
    So here we make a pre-pass to compute the sizes of the value files
    we will need.

    return a list of Work tuples each containing a list of (input) value file
    references and the size of the (output) value file they will be written to 
    """
    log = logging.getLogger("_prepare_work")
    prev_handoff_node_id = None
    prev_collection_id = None
    work_items = list()
    work_item = _work_template._make(list(), 0)
    for reference in _query_value_file_references(
        connection, [row.id for row in value_file_rows]
    ):
        if reference.handoff_node_id is not None:
            # at least one distinct value file per handoff node
            if reference.handoff_node_id != prev_handoff_node_id:
                if prev_handoff_node_id is not None:
                    log.debug(
                        "handoff node {0}; {1} entries, {2} bytes".format(
                            prev_handoff_node_id, len(work_item.references),
                            work_item.value_file_size
                        )
                    )
                    assert len(work_item.references) > 0
                    work_items.append(work_item)
                    work_item = _work_template._make(list(), 0)
                prev_handoff_node_id = reference.handoff_node_id
        elif reference.collection_id != prev_collection_id:
            if prev_handoff_node_id is not None:
                log.debug(
                    "handoff node {0}; {1} entries, {2} bytes".format(
                        prev_handoff_node_id, len(work_item.references),
                            work_item.value_file_size
                    )
                )
                assert len(work_item.references) > 0
                work_items.append(work_item)
                work_item = _work_template._make(list(), 0)
                prev_handoff_node_id = None

            # at least one distinct value file per collection_id
            if prev_collection_id is not None:
                log.debug(
                    "collection {0}; {1} entries, {2} bytes".format(
                        prev_collection_id, len(work_item.references),
                            work_item.value_file_size
                    )
                )
                assert len(work_item.references) > 0
                work_items.append(work_item)
                work_item = _work_template._make(list(), 0)

            prev_collection_id = reference.collection_id

        # if this write would put us over the max size,
        # start a new output value file
        expected_size = work_item.value_file_size + reference.sequence_size
        if expected_size > _max_value_file_size:
            log.debug("starting new work item due to size {0}".format(
                expected_size))
            work_items.append(work_item)
            work_item = _work_template._make(list(), 0)

        work_item.references.append(reference)
        work_item.value_file_size += reference.sequence_size
    
    if prev_handoff_node_id is not None:
        log.debug(
            "final handoff node {0}; {1} entries, {2} bytes".format(
                prev_handoff_node_id, len(work_item.references),
                work_item.value_file_size
            )
        )
        
    if prev_collection_id is not None:
        log.debug(
            "final collection {0}; {1} entries, {2} bytes".format(
                prev_collection_id, len(work_item.references),
                work_item.value_file_size
            )
        )

    work_items.append(work_item)
    return work_items    

def _defrag_pass(connection, file_space_info):
    """
    Make a single defrag pass
    Open target value files as follows:

     * one distinct value file per handoff node, regardless of collection_id
     * one distinct value file per collection_id

    return the number of bytes defragged
    """
    log = logging.getLogger("_defrag_pass")
    paths_to_unlink = list()
    bytes_defragged = 0

    defraggable_bytes, all_value_file_rows = _identify_defrag_candidates(
        connection
    )
    if defraggable_bytes == 0:
        return (paths_to_unlink, bytes_defragged, )

    input_value_files = dict()
    value_file_rows = list()
    for value_file_row in all_value_file_rows:
        try:
            input_value_file = InputValueFile(_repository_path, value_file_row)
        except IOError as instance:
            log.error("Error opening {0} {1}".format(value_file_row, instance))
            continue

        # Ticket #5855: we should only iterate over the value files we were 
        # actually able to open.
        value_file_rows.append(value_file_row)
        input_value_files[input_value_file.value_file_id] = input_value_file

    work_items = _prepare_work(connection, value_file_rows)

    for work_item in work_items:
        space_id, space_path = \
            find_least_volume_space_id_and_path("storage", file_space_info)

        # Ticket #5866 Nimbus.io should avoid filling disks to capacity
        available_space = available_space_on_volume(space_path)
        if available_space - work_item.value_file_size < _min_destination_space:
            log.warn("Insufficient space: available {0}, value_file {1}, "
                     "minimum {2}".format(
                available_space, work_item.value_file_size, 
                _min_destination_space))
            raise DefraggerInsufficentSpaceError("insufficient space")

        log.debug(
            "opening value file at space_id {0}, {1}".format(
                space_id, space_path
            )
        )

        output_value_file = OutputValueFile(connection, 
                                            space_id, 
                                            _repository_path,
                                            work_item.value_file_size)

        for reference in work_item.references:
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
                # TODO: - insert into repair table

                # Ticket #5855: log the error and copy the data anyway. 
                # corrupted data is still better than lost data. 
                # It might be wrong by a single bit.

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

        assert output_value_file.size == work_item.value_file_size, (
            output_value_file.size, work_item.value_file_size, )
        output_value_file.close()

    # close (and delete from the database) the old value files
    for input_value_file in input_value_files.values():
        input_value_file.close()

        # Ticket #5855: unlink the files after commit
        paths_to_unlink.append(input_value_file.value_file_path)

        connection.execute("""
            delete from nimbusio_node.value_file
            where id = %s""", [input_value_file.value_file_id, ])


    return (paths_to_unlink, bytes_defragged, )

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


        # try one defrag pass
        bytes_defragged = 0
        connection.begin_transaction()
        try:
            paths_to_unlink, bytes_defragged = \
                _defrag_pass(connection, file_space_info)

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

            # Ticket #5855: With regard to the database commit and the file 
            # unlink, of course we can't have both things happen atomically. 
            # So it's better to put the unlink after the transaction commit, 
            # because then the consequence of crashing and forgetting to do the 
            # unlink is just wasted disk space.
            for path_to_unlink in paths_to_unlink:        
                log.debug("unlinking {0}".format(path_to_unlink))
                os.unlink(path_to_unlink)

        log.info("bytes defragged = {0}".format(bytes_defragged))

        # if we didn't do anything on this pass...
        if bytes_defragged == 0:

            # exit if we're done and asked to do single pass
            if int(os.environ.get('NIMBUSIO_EXIT_WHEN_DONE', '0')):
                halt_event.set()

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

