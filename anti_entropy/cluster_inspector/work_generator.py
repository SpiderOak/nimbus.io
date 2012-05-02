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
        self.segment_row = None
        self.handoff_rows = list()
        self.advance()

    @property
    def segment_is_damaged(self):
        return self.segment_row is not None and \
                _row_key(self.segment_row) in self._damaged_set

    def advance(self):
        self.segment_row = None
        self.handoff_rows = list()
        while self._segment_file is not None:
            try:
                segment_row = retrieve_sized_pickle(self._segment_file)
            except EOFError:
                self._segment_file.close()
                self._segment_file = None
                break

            if segment_row["handoff_node_id"] is not None:
                self.handoff_rows.append(segment_row)
                continue

            self.segment_row = segment_row
            segment_row_key = _row_key(segment_row)
            for handoff_row in self.handoff_rows:
                assert _row_key(handoff_row) == segment_row_key

            break

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()

def _load_node_id_dict():
    central_connection = get_central_connection()
    cluster_row = get_cluster_row(central_connection)
    node_rows = get_node_rows(central_connection, cluster_row.id)
    central_connection.close()

    return dict([(node_row.name, node_row.id, ) for node_row in node_rows])

def generate_work(work_dir):
    """
    generate work data structures for segment audit
    We use dicts instead of named tuples for easy pickling

    audit_data (dict)
        "segment-status"
        "segment-data" (dict keyed by node_name)
            <node-name-1> (dict)
                "segment-row" : (dict)
                "is-damaged" : Boolean
            
    """
    log = logging.getLogger("generate_work")
    row_generators = dict()

    node_id_dict =_load_node_id_dict()

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
        
        # build an audit data structure
        audit_data = {"segment-status"  : anti_entropy_pre_audit,
                      "segment-data"    : dict()}

        handoff_rows = list()
        for node_name in _node_names:
            node_data = {"segment-row"  : None,
                         "is-damaged"   : False}
            audit_data["segment-data"][node_name] = node_data

            row_generator = row_generators[node_name]

            if row_generator.segment_row is None:
                continue

            row_key = _row_key(row_generator.segment_row)
            if row_key == minimum_row_key:
                node_data["segment-row"] = row_generator.segment_row
                node_data["is-damaged"] = row_generator.segment_is_damaged
                handoff_rows.extend(row_generator.handoff_rows)
                row_generator.advance()

        # now make a pass to fill in handoffs:
        # if we have a missing segment_row and some row generator has a handoff
        # for it, we substitute the handoff row. 
        # These substitutions can be recognized by handoff_node_id not None
        for node_name in _node_names:
            node_data = audit_data["segment-data"][node_name]
            if node_data["segment-row"] is None:
                node_id = node_id_dict[node_name]
                for handoff_row in handoff_rows:
                    if handoff_row["handoff_node_id"] == node_id:
                        assert _row_key(handoff_row) == minimum_row_key
                        node_data["segment-row"] = handoff_row
                        break

        yield audit_data


