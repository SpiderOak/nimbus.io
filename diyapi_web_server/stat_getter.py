# -*- coding: utf-8 -*-
"""
stat_getter.py

A class that performs a stat query.
"""
import uuid
from collections import defaultdict

import gevent
from gevent.pool import GreenletSet

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    StatFailedError,
)


class StatGetter(object):
    """Performs a stat query."""
    def __init__(self, database_clients, agreement_level):
        self.database_clients = database_clients
        self.agreement_level = agreement_level
        self._pending = GreenletSet()
        self._done = []

    def _join(self, timeout):
        self._pending.join(timeout)
        # make sure _done_link gets run first by cooperating
        gevent.sleep(0)
        if len(self._done) < self.agreement_level:
            raise StatFailedError()
        self._pending.kill()

    def _done_link(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            return
        if task.successful():
            self._done.append(task)

    def _spawn(self, database_client, run, *args):
        task = self._pending.spawn(run, *args)
        task.link(self._done_link)
        task.database_client = database_client
        return task

    def stat(self, avatar_id, path, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for database_client in self.database_clients:
            request_id = uuid.uuid1().hex
            self._spawn(
                database_client,
                database_client.stat,
                request_id,
                avatar_id,
                path
            )
        self._join(timeout)
        result = defaultdict(int)
        for task in self._done:
            key = frozenset(task.value.items())
            result[key] += 1
            if result[key] >= self.agreement_level:
                return task.value
        raise StatFailedError()
