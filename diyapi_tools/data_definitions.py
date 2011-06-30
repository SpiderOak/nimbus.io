# -*- coding: utf-8 -*-
"""
data_definitions.py

common data definitions
"""

from collections import namedtuple
from datetime import datetime
import os.path
import re

# datetime.datetime(2011, 6, 30, 13, 52, 34, 720271)
_timestamp_repr_re = re.compile(r"""
^datetime.datetime\(
(?P<year>\d{4})         #year
,\s
(?P<month>\d{1,2})      #month
,\s
(?P<day>\d{1,2})        #day
,\s
(?P<hour>\d{1,2})       #hour
,\s
(?P<minute>\d{1,2})     #minute
,\s
(?P<second>\d{1,2})     #second
,\s
(?P<microsecond>\d+)    #microsecond
\)$
""", re.VERBOSE)

def compute_value_file_path(repository_path, value_file_id):
    return os.path.join(
        repository_path, 
        "%03d" % (value_file_id % 1000), 
        "%08d" % value_file_id
    )

def parse_timestamp_repr(timestamp_repr):
    match_object = _timestamp_repr_re.match(timestamp_repr)
    if match_object is None:
        raise ValueError("unparsable timestamp '%s'" % (timestamp_repr, ))

    timestamp = datetime(
        year=int(match_object.group("year")),
        month=int(match_object.group("month")),
        day=int(match_object.group("day")),
        hour=int(match_object.group("hour")),
        minute=int(match_object.group("minute")),
        second=int(match_object.group("second")),
        microsecond=int(match_object.group("microsecond"))
    )

    return timestamp

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

