# -*- coding: utf-8 -*-
"""
stat_getter.py

A class that performs a stat query.
"""
import logging

from web_server.local_database_util import current_status_of_key, \
        current_status_of_version

class StatGetter(object):
    """Performs a stat query."""
    def __init__(self, node_local_connection):
        self._node_local_connection = node_local_connection
        self._log = logging.getLogger("StatGetter")

    def stat(self, collection_id, key, version_id=None):
        self._log.debug("collection_id=%s, key=%s, version_id=%r" % (
            collection_id, key, version_id
        ))
        
        if version_id is None:
            status_rows = current_status_of_key(
                self._node_local_connection,
                collection_id,
                key,
            )
        else:
            status_rows = current_status_of_version(
                self._node_local_connection,
                version_id
            )

        return status_rows

