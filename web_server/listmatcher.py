# -*- coding: utf-8 -*-
"""
listmatcher.py

listmatch query.
"""
import uuid

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
        select key, version_identifier, timestamp, file_tombstone 
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
    for row in result[:max_keys]:
        (key, version_identifier_bytes, timestamp, tombstone) = row
        if key == prev_key:
            continue
        prev_key = key
        if tombstone:
            break
        version_identifier = uuid.UUID(bytes=version_identifier_bytes)
        key_list.append(
            {"key" : key, 
             "version_identifier_hex" : version_identifier.hex, 
             "timestamp_repr" : repr(timestamp)}
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
    version_id_marker=""
):
    """
    get the most recent row (highest timestamp) for each matching key
    and version, exclude tombstones
    """
    # ask for one more than max_keys, so we can tell if we are truncated
    max_keys = int(max_keys)
    request_count = max_keys + 1
    result = connection.fetch_all_rows(
        """
        select key, version_identifier, timestamp, file_tombstone 
        from nimbusio_node.segment
        where collection_id = %s
        and handoff_node_id is null
        and key like %s
        and key > %s
        and version_identifier > %s
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
    prev_key = None
    prev_version_identifier_bytes = None
    for row in result[:max_keys]:
        (key, version_identifier_bytes, timestamp, tombstone) = row
        if key == prev_key and \
           version_identifier_bytes == prev_version_identifier_bytes:
            continue
        prev_key = key
        prev_version_identifier_bytes = version_identifier_bytes
        if tombstone:
           break 
        version_identifier = uuid.UUID(bytes=version_identifier_bytes)
        key_list.append(
            {"key" : key, 
             "version_identifier_hex" : version_identifier.hex, 
             "timestamp_repr" : repr(timestamp)}
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

