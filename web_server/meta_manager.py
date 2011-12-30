# -*- coding: utf-8 -*-
"""
meta_manager.py

functions for accessing meta data
"""
from web_server.local_database_util import current_status_of_key

_retrieve_meta_query = """
    select meta_key, meta_value from nimbusio_node.meta where
    collection_id = %s and segment_id = %s
""".strip()

def retrieve_meta(connection, collection_id, key):
    """
    get a dict of meta data associated with the segment
    """
    # TODO: find a non-blocking way to do this
    # TODO: don't just use the local node, it might be wrong
    _conjoined_row, segment_rows = current_status_of_key(
        connection, collection_id, key
    )
    if len(segment_rows) == 0 or segment_rows[0].file_tombstone:
        return None

    return dict(connection.fetch_all_rows(
        _retrieve_meta_query, [collection_id, segment_rows[0].id, ]
    ))

