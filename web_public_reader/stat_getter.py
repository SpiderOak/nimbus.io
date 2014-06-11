# -*- coding: utf-8 -*-
"""
stat_getter.py

A class that performs a stat query.
"""
import logging
import os

from segment_visibility.sql_factory import version_for_key

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

def get_last_modified_and_content_length(interaction_pool,
                                         collection_id, 
                                         versioned,
                                         key, 
                                         version_id=None):

    log = logging.getLogger("get_last_modified_and_content_length")
    sql_text = version_for_key(collection_id, 
                               versioned=versioned, 
                               key=key,
                               unified_id=version_id)

    log.debug("sql_text = '{0}'".format(sql_text))

    args = {"collection_id" : collection_id,
            "key"           : key, 
            "unified_id"    : version_id}

    async_result = interaction_pool.run(interaction=sql_text.encode("utf-8"),
                                        interaction_args=args,
                                        pool=_local_node_name)
    result = async_result.get()

    if len(result) == 0:
        return None, None

    last_modified, content_length = \
        last_modified_and_content_length_from_key_rows(result)

    log.debug("collection_id={0}, key={1}, version_id={2}, last_modified={3}, " 
              "content_length={4}".format(collection_id, 
                                          key,
                                          version_id,
                                          last_modified,
                                          content_length))
    return last_modified, content_length

def last_modified_and_content_length_from_key_rows(result):
    last_modified = None
    content_length = None
    if len(result) > 0:
        for row in result:
            if last_modified is None:
                last_modified = row["timestamp"]
            else:
                last_modified = min(last_modified, row["timestamp"])
            if content_length is None:
                content_length = row["file_size"]
            else:
                content_length += row["file_size"]

    return last_modified, content_length

