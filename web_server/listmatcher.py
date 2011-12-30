# -*- coding: utf-8 -*-
"""
listmatcher.py

listmatch query.
"""

def listmatch(
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
        select key, timestamp, file_tombstone 
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
    for (key, _, tombstone) in result[:max_keys]:
        if key == prev_key:
            continue
        prev_key = key
        if tombstone:
            continue
        key_list.append(key)

    if delimiter == "":
        return {"keys" : key_list, "truncated" : truncated} 

    # XXX: there may be some SQL way to do this efficiently
    prefix_set = set()
    for key in key_list:
        stub = key[len(prefix):]
        delimiter_pos = stub.find(delimiter)
        if delimiter_pos > 0:
            prefix_set.add(stub[:delimiter_pos+1])

    return {"prefixes" : list(prefix_set), "truncated" : truncated}

