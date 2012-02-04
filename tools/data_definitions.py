# -*- coding: utf-8 -*-
"""
data_definitions.py

common data definitions
"""

from collections import namedtuple
from datetime import datetime
import os.path
import re
import time

# our internal message format
message_format = namedtuple("Message", "ident control body")

def random_string(size):
    return os.urandom(size)

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

nimbus_meta_prefix = "__nimbus_io__"

def compute_value_file_path(repository_path, value_file_id):
    return os.path.join(
        repository_path, 
        "%03d" % (value_file_id % 1000), 
        "%08d" % value_file_id
    )

def create_priority():
    return int(time.time())

def create_timestamp():
    return datetime.utcnow()

def parse_timestamp_repr(timestamp_repr):
    """
    We can't send a timestamp pbject over JSON, so we send the repr
    and parse that to re-create the object
    """
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

def parse_conjoined_part(conjoined_part):
    return (0 if conjoined_part is None else conjoined_part) 

cluster_row_template = namedtuple("ClusterRow", [
    "id",
    "name",
    "node_count",
    "replication_level"]
)

node_row_template = namedtuple("NodeRow", [
    "id",
    "node_number_in_cluster",
    "name",
    "hostname",
    "offline"]
)

value_file_template = namedtuple("ValueFile", [
    "id",
    "creation_time",
    "close_time",
    "size",
    "hash",
    "segment_sequence_count",
    "min_segment_id",
    "max_segment_id",
    "distinct_collection_count",
    "collection_ids",
    "garbage_size_estimate",
    "fragmentation_estimate",
    "last_cleanup_check_time",
    "last_integrity_check_time"]
)

segment_row_template = namedtuple(
    "SegmentRow", [
        "id",
        "collection_id",
        "key",
        "unified_id",
        "timestamp",
        "segment_num",
        "conjoined_unified_id",
        "conjoined_part",
        "file_size",
        "file_adler32",
        "file_hash",
        "file_tombstone",
        "file_tombstone_unified_id",
        "handoff_node_id",
    ]
)

segment_sequence_template = namedtuple(
    "SegmentSequence", [
        "collection_id",
        "segment_id",
        "zfec_padding_size",
        "value_file_id",
        "sequence_num",
        "value_file_offset",
        "size",
        "hash",
        "adler32",
    ]
)

meta_row_template = namedtuple(
    "MetaRow", [
        "collection_id",
        "segment_id",
        "meta_key",
        "meta_value",
        "timestamp",
    ]
)

conjoined_row_template = namedtuple(
    "ConjoinedRow", [
        "id",
        "collection_id",
        "key",
        "unified_id",
        "create_timestamp",
        "abort_timestamp",
        "complete_timestamp",
        "delete_timestamp",
        "combined_size",
        "combined_hash",
    ]
)

