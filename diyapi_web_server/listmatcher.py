# -*- coding: utf-8 -*-
"""
listmatcher.py

A class that performs a listmatch query.
"""
from collections import defaultdict

import gevent
from gevent.pool import GreenletSet

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    ListmatchFailedError,
)


class Listmatcher(object):
    """Performs a listmatch query."""
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
            raise ListmatchFailedError()
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

    def listmatch(self, avatar_id, prefix, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for database_client in self.database_clients:
            self._spawn(
                database_client,
                database_client.listmatch,
                avatar_id,
                prefix
            )
        self._join(timeout)
        result = defaultdict(int)
        for task in self._done:
            for key in task.value:
                result[key] += 1
        return [key for key in sorted(result)
                if result[key] >= self.agreement_level]

