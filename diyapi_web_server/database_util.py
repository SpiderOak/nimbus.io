# -*- coding: utf-8 -*-
"""
database_util.py

utility routines for the node local database
"""
import time

from diyapi_tools.data_definitions import segment_row_template, \
        convert_segment_row

def most_recent_timestamp_for_key(connection, avatar_id, key):
    """
    Retrieve the row from the segment table that has the most recent
    timestamp.
    """
    result = connection.fetch_one_row("""
        select %s from diy.segment 
        where avatar_id = %%s and key = %%s and handoff_node_id is null
        order by timestamp desc
        limit 1
    """ % (",".join(segment_row_template._fields), ), [avatar_id, key, ])

    if result is None:
        return None

    return convert_segment_row(result)

def segment_row_for_key(connection, avatar_id, key, timestamp, segment_num):
    """
    Retrieve the row from the segment table
    """
    result = connection.fetch_one_row("""
        select %s from diy.segment 
        where avatar_id = %%s 
        and key = %%s 
        and timestamp=%%s::timestamp,
        and segment_num=%%s
    """ % (",".join(segment_row_template._fields), ), 
        [avatar_id, key, timestamp, segment_num])

    if result is None:
        return None

    return convert_segment_row(result)

