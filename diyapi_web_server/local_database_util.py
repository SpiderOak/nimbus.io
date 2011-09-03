# -*- coding: utf-8 -*-
"""
local_database_util.py

utility routines for the node local database
"""

from diyapi_tools.data_definitions import segment_row_template

def most_recent_timestamp_for_key(connection, collection_id, key):
    """
    Retrieve the row from the segment table that has the most recent
    timestamp.
    """
    result = connection.fetch_one_row("""
        select %s from nimbusio_node.segment 
        where collection_id = %%s and key = %%s and handoff_node_id is null
        order by timestamp desc
        limit 1
    """ % (",".join(segment_row_template._fields), ), [collection_id, key, ])

    if result is None:
        return None

    return segment_row_template._make(result)

def segment_row_for_key(
    connection, collection_id, key, timestamp, segment_num
):
    """
    Retrieve the row from the segment table
    """
    result = connection.fetch_one_row("""
        select %s from nimbusio_node.segment 
        where collection_id = %%s 
        and key = %%s 
        and timestamp=%%s::timestamp,
        and segment_num=%%s
    """ % (",".join(segment_row_template._fields), ), 
        [collection_id, key, timestamp, segment_num])

    if result is None:
        return None

    return segment_row_template._make(result)

