# -*- coding: utf-8 -*-
"""
listmatcher.py

listmatch query.
"""

from collections import namedtuple

_keys_entry = namedtuple("KeysEntry", [
    "key", "unified_id", "timestamp", "file_tombstone"]
)
                        
_versions_entry = namedtuple("VersionsEntry", [
    "key", 
    "unified_id", 
    "timestamp", 
    "file_tombstone", 
    "file_tombstone_unified_id"]
)

def list_keys(
    connection, 
    collection_id, 
    prefix="", 
    max_keys=1000, 
    delimiter="",
    marker=""
):
    """
    get the most recent row (highest timestamp) for each matching key
    exclude tombstones
    """
    # ask for one more than max_keys, so we can tell if we are truncated
    max_keys = int(max_keys)
    request_count = max_keys + 1
    result = connection.fetch_all_rows(
        """
        select key, unified_id, timestamp, file_tombstone
        from nimbusio_node.segment
        where collection_id = %s
        and handoff_node_id is null
        and key like %s
        and key > %s
        order by key asc, timestamp desc
        limit %s
        """.strip(),
        [collection_id, "%s%%" % prefix, marker, request_count, ]
    )

    truncated = len(result) == request_count
    key_list = list()
    prev_key = None
    for raw_row in result[:max_keys]:
        row = _keys_entry._make(raw_row)
        if row.key == prev_key:
            continue
        prev_key = row.key
        if row.file_tombstone:
            continue
        key_list.append(
            {"key" : row.key, 
             "version_identifier" : row.unified_id, 
             "timestamp_repr" : repr(row.timestamp)}
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

def list_versions(
    connection, 
    collection_id, 
    prefix="", 
    max_keys=1000, 
    delimiter="",
    key_marker="",
    version_id_marker_str=""
):
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

    result = connection.fetch_all_rows(
        """
        select key, unified_id, timestamp, file_tombstone, 
            file_tombstone_unified_id
        from nimbusio_node.segment
        where collection_id = %s
        and handoff_node_id is null
        and key like %s
        and key > %s
        and unified_id > %s
        order by key asc, timestamp desc
        limit %s
        """.strip(),
        [collection_id, 
         "%s%%" % prefix, 
         key_marker, 
         version_id_marker, 
         request_count, ]
    )

    truncated = len(result) == request_count
    key_list = list()
    tombstone_key = None
    tombstone_unified_id = None
    for raw_row in result[:max_keys]:
        row = _versions_entry._make(raw_row)
        if tombstone_key is not None and row.key == tombstone_key:
            continue
        if tombstone_unified_id is not None and \
           row.file_tomebstone_unified_id == tombstone_unified_id:
            tombstone_unified_id = None
            continue
        if row.file_tombstone:
            if row.file_tombstone_unified_id is None:
                tombstone_key = row.key
                tombstone_unified_id = None
            else:
                tombstone_key = None
                tombstone_unified_id = row.file_tombstone_unified_id
            continue 

        key_list.append(
            {"key" : row.key, 
             "version_identifier" : row.unified_id, 
             "timestamp_repr" : repr(row.timestamp)}
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

