# -*- coding: utf-8 -*-
"""
space_usage_getter.py

A class that performs a space_usage query.
"""

import gevent

from web_public_reader.exceptions import (
    AlreadyInProgress,
    SpaceUsageFailedError,
)


class SpaceUsageGetter(object):
    """Performs a space_usage query."""
    def __init__(self, accounting_server):
        self.accounting_server = accounting_server

    def get_space_usage(self, collection_name, timeout=None):
        task = gevent.spawn(
            self.accounting_server.get_space_usage,
            collection_name
        )

        try:
            usage = task.get(timeout=timeout)
        except gevent.Timeout:
            raise SpaceUsageFailedError()

        return usage

