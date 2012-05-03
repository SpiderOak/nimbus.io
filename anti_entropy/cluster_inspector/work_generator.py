# -*- coding: utf-8 -*-
"""
work_generator.py

generate work packets from segment files retrieved by pullers
"""
import gzip
import os
import logging

from tools.data_definitions import segment_row_template
from tools.database_connection import get_central_connection
from tools.sized_pickle import retrieve_sized_pickle

from web_server.central_database_util import get_cluster_row, \
        get_node_rows

from anti_entropy.anti_entropy_util import anti_entropy_pre_audit

from anti_entropy.cluster_inspector.util import compute_segment_file_path, \
        compute_damaged_segment_file_path

def _row_key(row):
    return (row["unified_id"], row["conjoined_part"], )

def _row_key_check(segment_data):
    row_key_set = set()
    for entry in segment_data.values():
        if entry["segment-row"] is None:
            row_key_set.add(None)
        else:
            row_key_set.add(_row_key(entry["segment-row"]))
    assert len(row_key_set) == 1, str(row_key_set)

class NodeRowGenerator(object):
    """
    generate segment row information for one node
    """
    def __init__(self, work_dir, node_name):
        path = compute_segment_file_path(work_dir, node_name)
        self._segment_file = gzip.GzipFile(filename=path, mode="rb")
        path = compute_damaged_segment_file_path(work_dir, node_name)
        self._damaged_file = gzip.GzipFile(filename=path, mode="rb")
        self.segment_dict = None
        try:
            self._damaged_dict = retrieve_sized_pickle(self._damaged_file)
        except EOFError:
            self._damaged_dict = None
        self.advance()

    def advance(self):
        try:
            self.segment_dict = retrieve_sized_pickle(self._segment_file)
        except EOFError:
            self.segment_dict = None
            self._segment_file.close()
            self._segment_file = None
            return

        segment_row_key = _row_key(self.segment_dict)

        while self._damaged_dict is not None and \
              segment_row_key > _row_key(self._damaged_dict):
            try:
                self._damaged_dict = retrieve_sized_pickle(self._damaged_file)
            except EOFError:
                self._damaged_dict = None

        if self._damaged_dict is None:
            self.segment_dict["damaged_sequence_numbers"] = list()
            return

        if segment_row_key < _row_key(self._damaged_dict):
            self.segment_dict["damaged_sequence_numbers"] = list()
            return

        if segment_row_key == _row_key(self._damaged_dict):
            self.segment_dict["damaged_sequence_numbers"] = \
                self._damaged_dict["sequence_numbers"]

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()

def generate_work(work_dir):
    """
    generate work data structures for segment audit
    We use dicts instead of named tuples for easy pickling
    We yield a tuple of ((unified_id, conjoined_part), status, [segment_rows],)
    with segment rows in order of node_name
    """
    log = logging.getLogger("generate_work")
    row_generators = [NodeRowGenerator(work_dir, n) for n in _node_names]

    while True:

        # find the minimum row_key (unified_id, conjoined_part)
        minimum_row_key = None
        for row_generator in row_generators:
            if row_generator.segment_dict is None:
                continue

            row_key = _row_key(row_generator.segment_dict)
            if minimum_row_key is None:
                minimum_row_key = row_key
            else:
                minimum_row_key = min(minimum_row_key, row_key)

        # if we don't find one, we have used up all segment rows
        if minimum_row_key is None:
            raise StopIteration()
        
        segment_data = list()
        for row_generator in row_generators:
            if row_generator.segment_dict is None:
                segment_data.append(None)
                continue

            row_key = _row_key(row_generator.segment_dict)
            if row_key != minimum_row_key:
                assert row_key > minimum_row_key
                segment_data.append(None)
                continue

            segment_data.append(row_generator.segment_dict)
            row_generator.advance()

        yield (minimum_row_key, anti_entropy_pre_audit, segment_data, )

