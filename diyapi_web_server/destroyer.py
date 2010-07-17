# -*- coding: utf-8 -*-
"""
destroyer.py

A class that performs a destroy query on all data writers.
"""
import logging
import uuid

import gevent
from gevent.pool import GreenletSet

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    DestroyFailedError,
)


class Destroyer(object):
    """Performs a destroy query on all data writers."""
    def __init__(self, data_writers):
        self.log = logging.getLogger('Destroyer()')
        self.data_writers = data_writers
        self._pending = GreenletSet()
        self._done = []

    def _join(self, timeout, handoff=False):
        self._pending.join(timeout, True)
        # make sure _done_link gets run first by cooperating
        gevent.sleep(0)
        if not self._pending:
            return
        raise DestroyFailedError()

    def _done_link(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            return
        self._done.append(task)

    def _spawn(self, segment_number, data_writer, run, *args):
        method_name = run.__name__
        task = self._pending.spawn(run, *args)
        task.rawlink(self._done_link)
        task.segment_number = segment_number
        task.data_writer = data_writer
        task.method_name = method_name
        return task

    def destroy(self, avatar_id, key, timestamp, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for i, data_writer in enumerate(self.data_writers):
            segment_number = i + 1
            request_id = uuid.uuid1().hex
            self._spawn(
                segment_number,
                data_writer,
                data_writer.destroy_key,
                request_id,
                avatar_id,
                timestamp,
                key,
                segment_number,
                0 # version_number
            )
        self._join(timeout)
        result = min([task.value for task in self._done])
        self._done = []
        return result
