# -*- coding: utf-8 -*-
"""
rewrite_value_files.py
"""
from collections import defaultdict
import logging
import operator
import os

from tools.data_definitions import compute_value_file_path
from tools.output_value_file import OutputValueFile

_max_value_file_size = int(os.environ.get(
    "NIMBUS_IO_MAX_VALUE_FILE_SIZE", str(1024 ** 3))
)

def _allocate_output_value_files(connection, repository_path, refs):
    output_value_files = defaultdict(list)

    # Open temp files for each new value file, 
    # Open on the storage volume with the most free space.
    # pre-allocate it to the right size, 
    # madvise to WONTNEED. 

    # we assume refs are ordered by 
    # s.collection_id, ss.value_file_id, ss.value_file_offset
    # see generate_vlue_file_references
    work_collection_id = None
    work_value_file_id = None
    work_value_file_size = 0
    for ref in refs:
        if work_collection_id is None:
            work_collection_id = ref.collection_id
            work_value_file_id = ref.vallue_file_id
        elif ref.collection_id != work_collection_id:
            output_value_files[work_collection_id].append(
                OutputValueFile(connection, 
                                repository_path, 
                                expected_size=work_value_file_size))
            work_collection_id = ref.collection_id
            work_value_file_id = ref.vallue_file_id
            work_value_file_size = ref.value_file_size
        elif ref.value_file_id != work_value_file_id:
            expected_size =  work_value_file_size + ref.value_file_size
            if expected_size > _max_value_file_size:
                output_value_files[work_collection_id].append(
                    OutputValueFile(connection,
                                    repository_path,
                                    expected_size=work_value_file_size))
                work_value_file_id = ref.vallue_file_id
                work_value_file_size = ref.value_file_size
            else:
                work_value_file_size = expected_size

    if expected_size > 0:
        output_value_files[work_collection_id].append(
            OutputValueFile(connection, repository_path, work_value_file_size)
        )

    return output_value_files

def _process_batch(connection, repository_path, refs, value_file_data):
    log = logging.getLogger("_process_batch")

    # Determine the number and sizes of target files needed: 
    # At least one value file per collection ID, 
    # no file larger than the standard max size of a value file.
    output_value_files = _allocate_output_value_files(connection, 
                                                      repository_path, refs)

    # Sort the records by segment.collection_id, segment.key and 
    # segment.unified_id.
    refs.sort(key=operator.attrgetter("collection_id", "key", "unified_id"))

    # Within each target file, records sorted by key and unified_id.
    work_collection_id = None
    value_files = None
    index = 0
    for ref in refs:
        if work_collection_id is None:
            work_collection_id = ref.collection_id
            value_files = output_value_files[work_collection_id]
            index = 0
        elif ref.collection_id != work_collection_id:
            assert value_files[-1].size == value_files[-1].expected_size
            work_collection_id = ref.collection_id
            value_files = output_value_files[work_collection_id]
            index = 0
        elif value_files[index].size == value_files[index].expected_size:
            index += 1

        data_block = value_file_data[ref.value_file_id]
        data = data_block[
            ref.value_file_offset:ref.value_file_offset+ref.data_size
        ]
        data_md5 = hashlib.md5(data)
        if data_md5.digest() != bytes(ref.data_hash):
            log.error(
                "md5 mismatch {0} {1} {2} {3} {4} {5} {6} {7}".format(
                    ref.segment_id,
                    ref.collection_id, 
                    ref.key, 
                    ref.unified_id,
                    ref.sequence_num,
                    ref.value_file_id,
                    ref.value_file_offset,
                    ref.data_size
                )
            )
            #TODO - insert into repair table
            continue

        value_file_offset = value_files[index].size
        value_files[index].write_data_for_one_sequence(
            ref.collection_id, ref.segment_id, data
        )

        # adjust segment_sequence row
        connection.execute("""
            update nimbusio_node.segment_sequence
            set value_file_id = %s, value_file_offset = %s
            where collection_id = %s and segment_id = %s
            and sequence_num = %s
        """, [value_files[index].value_file_id, 
              value_file_offset,
              ref.collection_id,
              ref.segment_id,
              ref.sequence_num])

    # close al the output value files, forcing database update
    for value_files in output_value_files.values():
        for value_file in value_files:
            value_file.close()

def _remove_old_value_files(repository_path, value_file_ids):
    log = logging.getLogger("_remove_old_value_files")
    for value_file_id in value_file_ids:
        value_file_path = \
                compute_value_file_path(repository_path, value_file_id)
        try:
            os.unlink(value_file_path)
        except Exception:       
            log.exception

def rewrite_value_files(options, connection, repository_path, ref_generator):
    log = logging.getLogger("_rewrite_value_files")
    max_sort_mem = options.max_sort_mem * 1024 ** 3

    batch_size = 0
    refs = list()
    value_file_data = dict()

    while True:

        try:
            ref = next(ref_generator)
        except StopIteration:
            break
        log.debug("{0}".format(ref))

        # this should be the start of a partition
        assert(ref.value_row_num == 1, ref)

        if batch_size + ref.value_file_size > max_sort_mem:
            connection.execute("begin", [])
            try:
                _process_batch(
                    connection, repository_path, refs, value_file_data)
            except Exception:
                connection.rollback()
                raise
            connection.commit()
            _remove_old_value_files(repositiry_path, value_file_data.keys())

            batch_size = 0
            refs = list()
            value_file_data = dict()

        # get the value file data
        # TODO: we should only store the actual references from the files into 
        # memory, not keep the whole files into memory.  Keeping the whole file 
        # in memory means we're using memory for parts of the files that are 
        # garbage, effectively decreasing the size of our output sort batch.  
        # We could end up with very small outputs from each batch if a large 
        # portion of the input value files are garbage.
        assert(ref.value_file_id not in value_file_data)
        value_file_path = \
                compute_value_file_path(repository_path, ref.value_file_id)
        with open(value_file_path, "rb") as input_file:
            value_file_data[ref.value_file_id] = input_file.read()

        # load up the refs for this partition
        refs.append(ref)
        for _ in range(ref.value_row_count-1):
            refs.append(next(ref_generator)) 

    if len(refs) > 0:
        connection.execute("begin")
        try:
            _process_batch(connection, refs, value_file_data)
        except Exception:
            connection.rollback()
        connection.commit()
        _remove_old_value_files(value_file_data.keys())


