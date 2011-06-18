# -*- coding: utf-8 -*-
"""
data_definitions.py

common data definitions
"""

from collections import namedtuple

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

