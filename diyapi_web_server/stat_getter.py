# -*- coding: utf-8 -*-
"""
stat_getter.py

A class that performs a stat query.
"""

import gevent

from diyapi_web_server.database_util import most_recent_timestamp_for_key

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    StatFailedError,
)

class StatGetter(object):
    """Performs a stat query."""
    def __init__(self, node_local_connection):
        self._node_local_connection = node_local_connection

    def stat(self, avatar_id, key, timeout=None):
        return most_recent_timestamp_for_key(
            self._node_local_connection,
            avatar_id,
            key
        )


