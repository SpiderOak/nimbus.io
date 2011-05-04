# -*- coding: utf-8 -*-
"""
space_usage_getter.py

A class that performs a space_usage query.
"""

import gevent

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    SpaceUsageFailedError,
)


class SpaceUsageGetter(object):
    """Performs a space_usage query."""
    def __init__(self, accounting_server):
        self.accounting_server = accounting_server

    def get_space_usage(self, avatar_id, timeout=None):
        task = gevent.spawn(
            self.accounting_server.get_space_usage,
            avatar_id
        )
        try:
            usage = task.get(timeout=timeout)
        except gevent.Timeout:
            raise SpaceUsageFailedError()
        return usage

