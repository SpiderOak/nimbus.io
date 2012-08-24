# -*- coding: utf-8 -*-
"""
meta_manager.py

functions for accessing meta data
"""
import os
from tools.data_definitions import segment_status_final

from web_public_reader.local_database_util import current_status_of_key, \
        current_status_of_version

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_retrieve_meta_query = """
    select meta_key, meta_value from nimbusio_node.meta where
    collection_id = %s and segment_id = %s
""".strip()

def retrieve_meta(interaction_pool, collection_id, key, version_id=None):
    """
    get a dict of meta data associated with the segment
    """
    # TODO: find a non-blocking way to do this
    # TODO: don't just use the local node, it might be wrong

    if version_id is None:
        status_rows = current_status_of_key(interaction_pool, 
                                            collection_id, 
                                            key)
    else:
        status_rows = current_status_of_version(interaction_pool, version_id)

    if len(status_rows) == 0 or \
       status_rows[0].seg_status != segment_status_final:
        return None

    async_result = \
        interaction_pool.run(interaction=_retrieve_meta_query, 
                             interaction_args=[collection_id, 
                                               status_rows[0].seg_id],
                             pool=_local_node_name)

    result = async_result.get()
    return [(row["meta_key"], row["meta_value"],) for row in result]

