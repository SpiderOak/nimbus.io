# -*- coding: utf-8 -*-
"""
work_generator.py

generate work packets from segment files retrieved by pullers
"""
import gzip
import os
import logging

from tools.data_definitions import segment_row_template

from anti_entropy.cluster_inspector.sized_pickle import retrieve_sized_pickle
from anti_entropy.cluster_inspector.util import compute_segment_file_path, \
        compute_damaged_segment_file_path

class NodeRowGenerator(object):
    """
    generate segment row and damaged_segment row for one node
    """
    def __init__(self, work_dir, node_name):
        path = compute_segment_file_path(work_dir, node_name)
        self._segment_file = gzip.GzipFile(filename=path, mode="rb")
        self.segment_row = None
        self.advance()

    def advance(self):
        if self._segment_file is not None:
            try:
                data = retrieve_sized_pickle(self._segment_file)
            except EOFError:
                self._segment_file.close()
                self._segment_file = None
                self.segment_row = None
            else:
                self.segment_row=segment_row_template(**data)

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()

def _row_key(row):
    return (row.unified_id, row.conjoined_part, )

def generate_work(work_dir):
    log = logging.getLogger("generate_work")
    row_generators = dict()

    # create the generators (with initial advance)
    for node_name in _node_names:
        row_generators[node_name] = NodeRowGenerator(work_dir, node_name)

    while True:

        # find the minimum row_key (unified_id, conjoined_part)
        minimum_row_key = None
        for node_name in _node_names:
            row_generator = row_generators[node_name]

            if row_generator.segment_row is None:
                continue

            row_key = _row_key(row_generator.segment_row)
            if minimum_row_key is None:
                minimum_row_key = row_key
            else:
                minimum_row_key = min(minimum_row_key, row_key)

        # if we don't find one, we have used up all segment rows
        if minimum_row_key is None:
            raise StopIteration()
        
        # build a work data structure
        work_data = dict()
        for node_name in _node_names:
            row_generator = row_generators[node_name]

            if row_generator.segment_row is None:
                work_data[node_name] = None
                continue

            row_key = _row_key(row_generator.segment_row)
            if row_key == minimum_row_key:
                work_data[node_name] = row_generator.segment_row
                row_generator.advance()
            else:
                work_data[node_name] = None

        yield work_data


