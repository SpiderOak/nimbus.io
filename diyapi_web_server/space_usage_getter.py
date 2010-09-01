# -*- coding: utf-8 -*-
"""
space_usage_getter.py

A class that performs a space_usage query.
"""
import uuid

import gevent
from gevent.pool import GreenletSet

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    SpaceUsageFailedError,
)


class SpaceUsageGetter(object):
    """Performs a space_usage query."""
    def __init__(self, accounting_server):
        self.accounting_server = accounting_server

    def get_space_usage(self, avatar_id, timeout=None):
        request_id = uuid.uuid1().hex
        task = gevent.spawn(
            self.accounting_server.get_space_usage,
            request_id,
            avatar_id
        )
        try:
            usage = task.get(timeout=timeout)
        except gevent.Timeout:
            raise SpaceUsageFailedError()
        return usage
