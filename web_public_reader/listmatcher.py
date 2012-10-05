# -*- coding: utf-8 -*-
"""
listmatcher.py

listmatch query.
"""
import os

from collections import namedtuple
from tools.data_definitions import segment_status_active, \
    segment_status_cancelled, \
    segment_status_final, \
    segment_status_tombstone, \
    http_timestamp_str

_keys_entry = namedtuple("KeysEntry", [
    "key", 
    "unified_id", 
    "status", 
    "timestamp",]
)
                        
_versions_entry = namedtuple("VersionsEntry", [
    "key", 
    "unified_id", 
    "status",
    "timestamp", 
    "file_tombstone_unified_id"]
)
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

def list_keys(interaction_pool, 
              collection_id, 
              prefix="", 
              max_keys=1000, 
              delimiter="",
              marker=""):
    """
    get the most recent row (highest timestamp) for each matching key
    exclude tombstones
    """
    # ask for one more than max_keys, so we can tell if we are truncated
    max_keys = int(max_keys)
    request_count = max_keys + 1
    async_result = interaction_pool.run(
        interaction="""
        select seg.key, seg.unified_id, seg.status, seg.timestamp
        from nimbusio_node.conjoined con right join nimbusio_node.segment seg 
        on con.unified_id = seg.unified_id
        where (con.create_timestamp is null 
            or con.complete_timestamp is not null)
        and con.delete_timestamp is null
        and seg.collection_id = %s
        and seg.handoff_node_id is null
        and seg.key like %s
        and seg.key > %s
        order by seg.key asc, seg.timestamp desc
        limit %s
        """.strip(),
        interaction_args=[collection_id, "%s%%" % prefix, 
                          marker, 
                          request_count, ],
        pool=_local_node_name,
    )
    result = async_result.get()

    truncated = len(result) == request_count
    key_list = list()
    prev_key = None
    for row in result[:max_keys]:
        if row["key"] == prev_key:
            continue
        if row["status"] in [segment_status_active, segment_status_cancelled]:
            continue
        prev_key = row["key"]
        if row["status"] == segment_status_tombstone:
            continue
        key_list.append(
            {"key"                : row["key"], 
             "version_identifier" : row["unified_id"], 
             "timestamp"          : http_timestamp_str(row["timestamp"])}
        )

    if delimiter == "":
        return {"key_data" : key_list, "truncated" : truncated} 

    # XXX: there may be some SQL way to do this efficiently
    prefix_set = set()
    for key_entry in key_list:
        delimiter_pos = key_entry["key"].find(delimiter, len(prefix))
        if delimiter_pos > 0:
            prefix_set.add(key_entry["key"][:delimiter_pos+1])

    return {"prefixes" : list(prefix_set), "truncated" : truncated}

def list_versions(interaction_pool, 
                  collection_id, 
                  prefix="", 
                  max_keys=1000, 
                  delimiter="",
                  key_marker="",
                  version_id_marker_str=""):
    """
    get the most recent row (highest timestamp) for each matching key
    and version, exclude tombstones
    """
    # ask for one more than max_keys, so we can tell if we are truncated
    max_keys = int(max_keys)
    request_count = max_keys + 1
    try:
        version_id_marker = int(version_id_marker_str)
    except ValueError:
        version_id_marker = 0

    async_result = interaction_pool.run(
        interaction="""
        select seg.key, seg.unified_id, seg.status, seg.timestamp, 
            seg.file_tombstone_unified_id
        from nimbusio_node.conjoined con right join nimbusio_node.segment seg
        on con.unified_id = seg.unified_id
        where (con.create_timestamp is null 
            or con.complete_timestamp is not null)
        and con.delete_timestamp is null
        and seg.collection_id = %s
        and seg.handoff_node_id is null
        and seg.key like %s
        and seg.key > %s
        and seg.unified_id > %s
        order by seg.key asc, seg.timestamp desc
        limit %s
        """.strip(),
        interaction_args=[collection_id, 
                          "%s%%" % prefix, 
                          key_marker, 
                          version_id_marker, 
                          request_count, ],
        pool=_local_node_name,
    )

    result = async_result.get()

    truncated = len(result) == request_count
    key_list = list()
    tombstone_key = None
    tombstone_unified_id = None
    for row in result[:max_keys]:
        if tombstone_key is not None and row["key"] == tombstone_key:
            continue
        if tombstone_unified_id is not None and \
           row["unified_id"] == tombstone_unified_id:
            tombstone_unified_id = None
            continue
        if row["status"] == segment_status_tombstone:
            if row["file_tombstone_unified_id"] is None:
                tombstone_key = row["key"]
                tombstone_unified_id = None
            else:
                tombstone_key = None
                tombstone_unified_id = row["file_tombstone_unified_id"]
            continue 
        if row["status"] in [segment_status_active, segment_status_cancelled]:
            continue

        key_list.append(
            {"key"                : row["key"], 
             "version_identifier" : row["unified_id"], 
             "timestamp"          : http_timestamp_str(row["timestamp"])}
        )

    if delimiter == "":
        return {"key_data" : key_list, "truncated" : truncated} 

    # XXX: there may be some SQL way to do this efficiently
    prefix_set = set()
    for key_entry in key_list:
        delimiter_pos = key_entry["key"].find(delimiter, len(prefix))
        if delimiter_pos > 0:
            prefix_set.add(key_entry["key"][:delimiter_pos+1])

    return {"prefixes" : list(prefix_set), "truncated" : truncated}

