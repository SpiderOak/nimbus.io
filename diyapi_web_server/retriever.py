# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import logging
import uuid

import gevent
from gevent.pool import GreenletSet

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    RetrieveFailedError,
)


class Retriever(object):
    """Retrieves data from data readers."""
    def __init__(self, data_readers, avatar_id, key, segments_needed):
        self.log = logging.getLogger(
            'Retriever(avatar_id=%d, key=%r)' % (
                avatar_id, key))
        self.data_readers = data_readers
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = 0
        self.segments_needed = segments_needed
        self.sequence_number = 0
        self.n_slices = None
        self._request_ids = {}
        self._pending = GreenletSet()
        self._done = []

    def _join(self, timeout):
        self._pending.join(timeout)
        # make sure _done_link gets run first by cooperating
        gevent.sleep(0)
        if self._pending:
            raise RetrieveFailedError()
        if len(self._done) < self.segments_needed:
            raise RetrieveFailedError()

    def _done_link(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            return
        if task.successful():
            self._done.append(task)
            if len(self._done) >= self.segments_needed:
                self._pending.kill()

    def _spawn(self, segment_number, data_reader, run, *args):
        task = self._pending.spawn(run, *args)
        task.link(self._done_link)
        task.segment_number = segment_number
        task.data_reader = data_reader
        return task

    def retrieve(self, timeout=None):
        if self._pending or self.n_slices is not None:
            raise AlreadyInProgress()
        for i, data_reader in enumerate(self.data_readers):
            segment_number = i + 1
            self._request_ids[segment_number] = uuid.uuid1().hex
            self._spawn(
                segment_number,
                data_reader,
                data_reader.retrieve_key_start,
                self._request_ids[segment_number],
                self.avatar_id,
                self.key,
                self.version_number,
                segment_number
            )
        self._join(timeout)        
        self.n_slices = self._done[0].value[0] # segment_count
        yield dict((task.segment_number, task.value[1])
                   for task in self._done[:self.segments_needed])
        self._done = []
        self.sequence_number += 1

        while self.sequence_number < self.n_slices - 1:
            for i, data_reader in enumerate(self.data_readers):
                segment_number = i + 1
                self._spawn(
                    segment_number,
                    data_reader,
                    data_reader.retrieve_key_next,
                    self._request_ids[segment_number],
                    self.sequence_number
                )
            self._join(timeout)
            yield dict((task.segment_number, task.value)
                       for task in self._done[:self.segments_needed])
            self._done = []
            self.sequence_number += 1

        if self.sequence_number >= self.n_slices:
            return
        for i, data_reader in enumerate(self.data_readers):
            segment_number = i + 1
            self._spawn(
                segment_number,
                data_reader,
                data_reader.retrieve_key_final,
                self._request_ids[segment_number],
                self.sequence_number
            )
        self._join(timeout)
        yield dict((task.segment_number, task.value)
                   for task in self._done[:self.segments_needed])
        self._done = []
