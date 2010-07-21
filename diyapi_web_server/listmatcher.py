# -*- coding: utf-8 -*-
"""
listmatcher.py

A class that performs a listmatch query.
"""
import uuid
from collections import defaultdict

import gevent
from gevent.pool import GreenletSet

from messages.database_listmatch import DatabaseListMatch

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    ListmatchFailedError,
)


class Listmatcher(object):
    """Performs a listmatch query."""
    def __init__(self, data_readers, agreement_level):
        self.data_readers = data_readers
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

    def _spawn(self, data_reader, run, *args):
        task = self._pending.spawn(run, *args)
        task.link(self._done_link)
        task.data_reader = data_reader
        return task

    def listmatch(self, avatar_id, prefix, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for data_reader in self.data_readers:
            request_id = uuid.uuid1().hex
            self._spawn(
                data_reader,
                data_reader.listmatch,
                request_id,
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
