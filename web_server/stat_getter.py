# -*- coding: utf-8 -*-
"""
stat_getter.py

A class that performs a stat query.
"""

import gevent

from web_server.local_database_util import most_recent_timestamp_for_key

class StatGetter(object):
    """Performs a stat query."""
    def __init__(self, node_local_connection):
        self._node_local_connection = node_local_connection

    def stat(self, collection_id, key, timeout=None):
        return most_recent_timestamp_for_key(
            self._node_local_connection,
            collection_id,
            key
        )


