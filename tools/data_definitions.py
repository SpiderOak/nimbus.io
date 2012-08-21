# -*- coding: utf-8 -*-
"""
data_definitions.py

common data definitions
"""

from collections import namedtuple
from datetime import datetime, timedelta
import os
import os.path
import re
import time

# our internal message format
message_format = namedtuple("Message", "ident control body")

def random_string(size):
    return os.urandom(size)

# the size we slice incoming data to send to the data_writers
incoming_slice_size = int(
    os.environ.get("NIMBUS_IO_SLICE_SIZE", str(10 * 1024 * 1024)))

# the size of data used for zfec encoding of a segment
block_size = 32 * 1024

# minimum number of nodes in a functioning cluster
min_node_count = 8

# the zfec algorithm takes a string of length L and encodes it 
# in M segments of which any K can be decoded to reproduce L
# In our case M = 10 (10 nodes) K = 8
def zfec_slice_size(data_size):
    return data_size // min_node_count

# the size of a zfec encoded block
encoded_block_slice_size = zfec_slice_size(block_size)

# we divide incoming files into N slices, 
# each one gets a segment_sequence row
# so we expect this batch to have N entries,
def compute_expected_slice_count(file_size, slice_size=incoming_slice_size):
    expected_slice_count = file_size // slice_size
    if file_size % slice_size != 0:
        expected_slice_count += 1
    return expected_slice_count

def _slice_generator(data, slice_size):
    start_pos = 0
    end_pos = slice_size
    while start_pos < len(data):
        yield data[start_pos:end_pos]
        start_pos = end_pos
        end_pos += slice_size

def block_generator(data):
    return _slice_generator(data, block_size)

def encoded_block_generator(data):
    return _slice_generator(data, encoded_block_slice_size)

def zfec_padding_size(data):
    modulus = len(data) % min_node_count
    return (0 if modulus == 0 else min_node_count - modulus)

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

def compute_value_file_path(repository_path, space_id, value_file_id):
    return os.path.join(
        repository_path, 
        str(space_id),
        "{:03d}".format(value_file_id % 1000), 
        "{:08d}".format(value_file_id)
    )

def create_priority():
    return int(time.time())

def create_timestamp():
    return datetime.utcnow()

_http_timestamp_format = "%a, %d %b %Y %H:%M:%S GMT"

def http_timestamp_str(timestamp):
    return timestamp.strftime(_http_timestamp_format)

def parse_http_timestamp(timestamp_str):
    # TODO: allow variant time formats
    return datetime.strptime(timestamp_str, _http_timestamp_format)

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

def parse_timedelta_str(interval_str):
    """
    crudely parse a string into keyword arguments for timedelta
    """
    kwargs = dict()
    for arg in interval_str.split(","):
        [name, value, ] = arg.split("=")
        kwargs[name.strip()] = int(value)
    return timedelta(**kwargs)

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

file_space_template = namedtuple("FileSpace", [
    "space_id",
    "purpose",
    "path",
    "volume",
    "creation_time"])

value_file_template = namedtuple("ValueFile", [
    "id",
    "space_id",
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

segment_status_active = "A"
segment_status_cancelled = "C"
segment_status_final = "F"
segment_status_tombstone = "T" 

segment_row_template = namedtuple(
    "SegmentRow", [
        "id",
        "collection_id",
        "key",
        "status", 
        "unified_id",
        "timestamp",
        "segment_num",
        "conjoined_part",
        "file_size",
        "file_adler32",
        "file_hash",
        "file_tombstone_unified_id",
        "source_node_id",
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
        "handoff_node_id",
    ]
)

damaged_segment_defective_sequence = "D"
damaged_segment_missing_sequence = "M"

