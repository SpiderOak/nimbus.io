# -*- coding: utf-8 -*-
"""
rewrite_value_files.py
"""
import logging

from tools.data_definitions import compute_value_file_path

def _process_batch(connection, refs, value_file_data):
    log = logging.getLogger("_process_batch")

def _remove_old_value_files(value_file_ids):
    log = logging.getLogger("_remove_old_value_files")

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
            _process_batch(connection, refs, value_file_data)
            _remove_old_value_files(value_file_data.keys())

            batch_size = 0
            refs = list()
            value_file_data = dict()

        # get the value file data
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
        _process_batch(connection, refs, value_file_data)


