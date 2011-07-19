# -*- coding: utf-8 -*-
"""
listmatcher.py

A class that performs a listmatch query.
"""
from collections import defaultdict

import gevent
import gevent.pool

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    ListmatchFailedError,
)


class Listmatcher(object):
    """Performs a listmatch query."""
    def __init__(self, node_local_connection):
        self._node_local_connection = node_local_connection

    def listmatch(self, avatar_id, prefix, timeout=None):
        return self._node_local_connection.fetch_all_rows(
            """
            select key from diy.segment
            where avatar_id = %s
            and file_tombstone = false
            and key like %s
            """.strip(),
            [avatar_id, "%s%%" % prefix, ]
        )

