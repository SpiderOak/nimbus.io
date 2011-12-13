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
        [collection_id, "%s%%" % prefix, marker, max_keys, ]
    )

    key_list = list()
    prev_key = None
    for (key, _, tombstone) in result:
        if key == prev_key:
            continue
        prev_key = key
        if tombstone:
            continue
        key_list.append(key)

    if delimiter == "":
        return key_list

    # XXX: there may be some SQL way to do this efficiently
    prefix_set = set()
    for key in key_list:
        stub = key[len(prefix):]
        delimiter_pos = stub.find(delimiter)
        if delimiter_pos > 0:
            prefix_set.add(stub[:delimiter_pos+1])
    return list(prefix_set)

