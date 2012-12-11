# -*- coding: utf-8 -*-
"""
operatonal_stats_redis_key.py

create key strings for redis for operational stats data
"""
from datetime import datetime

_key_prefix = "nimbus.io"
_key_type = "collection_ops_accounting"

def compute_key(node_name, timestamp, partial_key):
    """
    return a string suitable for use as a key to a redis hash
    """
    timestamp_template = "{0}.{1:02}.{2:02}.{3:02}.{4:02}"
    timestamp_string = timestamp_template.format(timestamp.year,
                                                 timestamp.month,
                                                 timestamp.day,
                                                 timestamp.hour,
                                                 timestamp.minute)
    return ".".join([_key_prefix, 
                     node_name,
                     _key_type,
                     timestamp_string, 
                     partial_key])

def compute_search_key(node_name):
    """
    return a string suitable for searching the redis keys on a node
    """
    return ".".join([_key_prefix, node_name, _key_type, "*"])

# nimbus.io.<node>.collection_ops_accounting.2012.12.04.22.12.archive_success
def parse_key(key):
    """
    return the node_name, timestamp, and partial_key
    """
    key_as_list = key.split(".")
    node_name = key_as_list[2]
    timestamp_list = key_as_list[4:9]
    timestamp = datetime(int(timestamp_list[0]),
                         int(timestamp_list[1]),
                         int(timestamp_list[2]),
                         hour=int(timestamp_list[3]),
                         minute=int(timestamp_list[4]))
    partial_key = key_as_list[9]

    return node_name, timestamp, partial_key

