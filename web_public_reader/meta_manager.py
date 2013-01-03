# -*- coding: utf-8 -*-
"""
meta_manager.py

functions for accessing meta data
"""
import os
from segment_visibility.sql_factory import version_for_key

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_retrieve_meta_query = """
    select meta_key, meta_value from nimbusio_node.meta where
    collection_id = %s and segment_id = %s
""".strip()

def retrieve_meta(interaction_pool, 
                  collection_id, 
                  versioned, 
                  key, 
                  version_id=None):
    """
    get a dict of meta data associated with the segment
    """
    # TODO: find a non-blocking way to do this
    # TODO: don't just use the local node, it might be wrong

    sql_text = version_for_key(collection_id, 
                               versioned=versioned, 
                               key=key,
                               unified_id=version_id)

    args = {"collection_id" : collection_id,
            "key"           : key, 
            "unified_id"    : version_id}

    async_result = interaction_pool.run(interaction=sql_text.encode("utf-8"),
                                        interaction_args=args,
                                        pool=_local_node_name)
    result = async_result.get()

    if len(result) == 0:
        return None

    async_result = \
        interaction_pool.run(interaction=_retrieve_meta_query, 
                             interaction_args=[collection_id, 
                                               result[0]["segment_id"]],
                             pool=_local_node_name)

    result = async_result.get()
    return [(row["meta_key"], row["meta_value"],) for row in result]

