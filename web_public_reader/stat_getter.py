# -*- coding: utf-8 -*-
"""
stat_getter.py

A class that performs a stat query.
"""
import logging

from tools.data_definitions import segment_status_final

from web_public_reader.local_database_util import current_status_of_key, \
        current_status_of_version

def get_last_modified_and_content_length(interaction_pool,
                                         collection_id, 
                                         key, 
                                         version_id=None):

    log = logging.getLogger("get_last_modified_and_content_length")
    if version_id is None:
        status_rows = current_status_of_key(interaction_pool,
                                            collection_id,
                                            key)
    else:
        status_rows = current_status_of_version(interaction_pool,
                                                version_id)

    last_modified, content_length = \
        last_modified_and_content_length_from_status_rows(status_rows)

    log.debug("collection_id={0}, key={1}, version_id={2}, last_modified={3}, " 
              "content_length={4}".format(collection_id, 
                                          key,
                                          version_id,
                                          last_modified,
                                          content_length))
    return last_modified, content_length

def last_modified_and_content_length_from_status_rows(status_rows):
    last_modified = None
    content_length = None
    if len(status_rows) > 0 and \
        status_rows[0].seg_status == segment_status_final:
        for status_row in status_rows:
            if last_modified is None:
                last_modified = status_row.seg_timestamp
            else:
                last_modified = min(last_modified, status_row.seg_timestamp)
            if content_length is None:
                content_length = status_row.seg_file_size
            else:
                content_length += status_row.seg_file_size

    
    return last_modified, content_length

