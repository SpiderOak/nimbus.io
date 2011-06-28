# -*- coding: utf-8 -*-
"""
data_definitions.py

common data definitions
"""

from collections import namedtuple
import os.path
import time

def compute_value_file_path(repository_path, value_file_id):
    return os.path.join(
        repository_path, 
        "%03d" % (value_file_id % 1000), 
        "%08d" % value_file_id
    )

value_file_template = namedtuple("ValueFile", [
    "id",
    "creation_time",
    "close_time",
    "size",
    "hash",
    "sequence_count",
    "min_segment_id",
    "max_segment_id",
    "distinct_avatar_count",
    "avatar_ids",
    "garbage_size_estimate",
    "fragmentation_estimate",
    "last_cleanup_check_time",
    "last_integrity_check_time"]
)

segment_row_template = namedtuple(
    "SegmentRow", [
        "id",
        "avatar_id",
        "key",
        "timestamp",
        "segment_num",
        "file_size",
        "file_adler32",
        "file_hash",
        "file_user_id",
        "file_group_id",
        "file_permissions",
        "file_tombstone",
        "handoff_node_id",
    ]
)

def convert_segment_row(row):
    raw_values = segment_row_template._make(row)
    # convert the psycopg2 timestamp to time.time() truncated to even second
    return raw_values._replace(
        timestamp=int(time.mktime(raw_values.timestamp.timetuple()))
    )

segment_sequence_template = namedtuple(
    "SegmentSequence", [
        "avatar_id",
        "segment_id",
        "value_file_id",
        "sequence_num",
        "value_file_offset",
        "size",
        "hash",
        "adler32",
    ]
)

