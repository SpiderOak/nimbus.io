# -*- coding: utf-8 -*-
"""
database_util.py

utility routines for the node local database
"""

def most_recent_timestamp_for_key(connection, avatar_id, key):
    """
    Retrieve the row form the segment table that has the most recent
    timestamp.
    This is the default for retrieving a key
    """
    from diyapi_tools.data_definitions import segment_row_template
    result = connection.fetch_one_row("""
        select %s from diy.segment 
        where avatar_id = %%s and key = %%s and handoff_node_id is null
        order by timestamp desc
    """ % (",".join(segment_row_template._fields), ), [avatar_id, key, ])

    if result is None:
        return None

    return segment_row_template._make(result)

