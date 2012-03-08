# -*- coding: utf-8 -*-
"""
meta_manager.py

functions for accessing meta data
"""
from tools.data_definitions import segment_status_final

from web_server.local_database_util import current_status_of_key, \
        current_status_of_version

_retrieve_meta_query = """
    select meta_key, meta_value from nimbusio_node.meta where
    collection_id = %s and segment_id = %s
""".strip()

def retrieve_meta(connection, collection_id, key, version_id=None):
    """
    get a dict of meta data associated with the segment
    """
    # TODO: find a non-blocking way to do this
    # TODO: don't just use the local node, it might be wrong

    if version_id is None:
        status_rows = current_status_of_key(connection, collection_id, key)
    else:
        status_rows = current_status_of_version(connection, version_id)

    if len(status_rows) == 0 or \
       status_rows[0].seg_status != segment_status_final:
        return None

    return dict(connection.fetch_all_rows(
        _retrieve_meta_query, [collection_id, status_rows[0].seg_id, ]
    ))

