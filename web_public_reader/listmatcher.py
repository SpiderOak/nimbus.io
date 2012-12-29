# -*- coding: utf-8 -*-
"""
listmatcher.py

listmatch query.
"""
import os

from tools.data_definitions import http_timestamp_str
from segment_visibility import sql_factory

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

def list_keys(interaction_pool, 
              collection_id, 
              versioned,
              prefix=None, 
              max_keys=1000, 
              delimiter="",
              marker=None):
    """
    retrieve infromation about keys which are visible: not deleted, etc
    """
    # ask for one more than max_keys, so we can tell if we are truncated
    max_keys = int(max_keys)
    request_count = max_keys + 1
    
    sql_text = sql_factory.list_keys(collection_id,
                                     versioned=versioned,
                                     prefix=prefix,
                                     key_marker=marker,
                                     limit=request_count)

    args = {"collection_id" : collection_id,
            "prefix"        : (prefix if prefix is not None else ""), 
            "key_marker"    : (marker if marker is not None else ""), }

    async_result = interaction_pool.run(interaction=sql_text.encode("utf-8"),
                                        interaction_args=args,
                                        pool=_local_node_name)
    result = async_result.get()

    truncated = len(result) == request_count
    key_list = list()
    for row in result:
        key_list.append(
            {"key"                : row["key"], 
            "version_identifier"  : row["unified_id"], 
            "timestamp"           : http_timestamp_str(row["timestamp"])})

    if delimiter == "":
        return {"key_data" : key_list, "truncated" : truncated} 

    # XXX: there may be some SQL way to do this efficiently
    prefix_set = set()
    offset = (len(prefix) if prefix is not None else 0)
    for key_entry in key_list:
        delimiter_pos = key_entry["key"].find(delimiter, offset)
        if delimiter_pos > 0:
            prefix_set.add(key_entry["key"][:delimiter_pos+1])

    return {"prefixes" : list(prefix_set), "truncated" : truncated}

def list_versions(interaction_pool, 
                  collection_id, 
                  versioned,
                  prefix=None, 
                  max_keys=1000, 
                  delimiter="",
                  key_marker=None,
                  version_id_marker_str=None):
    """
    retrieve infromation about versions which are visible: not deleted, etc
    """
    # ask for one more than max_keys, so we can tell if we are truncated
    max_keys = int(max_keys)
    request_count = max_keys + 1
    if version_id_marker_str is None:
        version_id_marker = 0
    else:
        try:
            version_id_marker = int(version_id_marker_str)
        except ValueError:
            version_id_marker = 0

    sql_text = sql_factory.list_versions(collection_id,
                                         versioned=versioned,
                                         prefix=prefix,
                                         key_marker=key_marker,
                                         version_marker=version_id_marker,
                                         limit=request_count)

    args = {"collection_id" : collection_id,
            "prefix"        : (prefix if prefix is not None else ""),
            "key_marker"    : (key_marker if key_marker is not None else ""),
            "version_marker": 
                (version_id_marker if version_id_marker is not None else 0), }

    async_result = interaction_pool.run(interaction=sql_text.encode("utf-8"),
                                        interaction_args=args, 
                                        pool=_local_node_name)

    result = async_result.get()

    truncated = len(result) == request_count
    key_list = list()
    for row in result:
        key_list.append(
            {"key"                : row["key"], 
            "version_identifier" : row["unified_id"], 
            "timestamp"          : http_timestamp_str(row["timestamp"])})

    if delimiter == "":
        return {"key_data" : key_list, "truncated" : truncated} 

    # XXX: there may be some SQL way to do this efficiently
    prefix_set = set()
    offset = (len(prefix) if prefix is not None else 0)
    for key_entry in key_list:
        delimiter_pos = key_entry["key"].find(delimiter, offset)
        if delimiter_pos > 0:
            prefix_set.add(key_entry["key"][:delimiter_pos+1])

    return {"prefixes" : list(prefix_set), "truncated" : truncated}

