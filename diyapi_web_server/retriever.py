# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import logging

import gevent
import gevent.pool

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    RetrieveFailedError,
)
from diyapi_web_server.database_util import most_recent_timestamp_for_key


class Retriever(object):
    """Retrieves data from data readers."""
    def __init__(
        self, 
        node_local_connection,
        data_readers, 
        avatar_id, 
        key, 
        segments_needed
    ):
        self.log = logging.getLogger("Retriever")
        self.log.info('avatar_id=%d, key=%r' % (avatar_id, key, ))
        self._node_local_connection = node_local_connection
        self.data_readers = data_readers
        self.avatar_id = avatar_id
        self.key = key
        self.segments_needed = segments_needed
        self._pending = gevent.pool.Group()
        self._done = []

    def _join(self, timeout):
        self._pending.join(timeout, raise_error=True)
        # make sure _done_link gets run first by cooperating
        gevent.sleep(0)
        if self._pending:
            raise RetrieveFailedError()
        if len(self._done) < self.segments_needed:
            raise RetrieveFailedError("too few segments done %s" % (
                len(self._done),
            ))

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
        if self._pending:
            raise AlreadyInProgress()

        # TODO: find a non-blocking way to do this
        file_info = most_recent_timestamp_for_key(
            self._node_local_connection , self.avatar_id, self.key
        )

        if file_info is None:
            raise RetrieveFailedError("key not found %s %s" % (
                self.avatar_id, self.key,
            ))

        if file_info.file_tombstone:
            raise RetrieveFailedError("key is deleted %s %s" % (
                self.avatar_id, self.key,
            ))

        for i, data_reader in enumerate(self.data_readers):
            segment_number = i + 1
            self._spawn(
                segment_number,
                data_reader,
                data_reader.retrieve_key_start,
                self.avatar_id,
                self.key,
                file_info.timestamp,
                segment_number
            )
        self._join(timeout)  

        # we expect retrieve_key_start to return the tuple
        # (<data-segment>, <completion-status>, )
        # where completion-status is a boolean

        yield dict((task.segment_number, task.value[0])
                   for task in self._done[:self.segments_needed])
        completed_list = \
            [task.value[1] for task in self._done[:self.segments_needed]]
        completed = all(completed_list)
        if completed:
            return
        if any(completed_list):
            raise RetrieveFailedError("inconsistent completed status %s" % (
                completed_list,
            ))
            
        while True:
            self._done = []
            for i, data_reader in enumerate(self.data_readers):
                segment_number = i + 1
                self._spawn(
                    segment_number,
                    data_reader,
                    data_reader.retrieve_key_next,
                    self.avatar_id,
                    self.key,
                    file_info.timestamp,
                    segment_number
                )
            self._join(timeout)

            # we expect retrieve_key_next to return the tuple
            # (<data-segment>, <completion-status>, )
            # where completion-status is a boolean

            yield dict((task.segment_number, task.value[0])
                       for task in self._done[:self.segments_needed])
            completed_list = \
                [task.value[1] for task in self._done[:self.segments_needed]]
            completed = all(completed_list)
            if completed:
                return
            if any(completed_list):
                raise RetrieveFailedError(
                    "inconsistent completed status %s" % (
                        completed_list,
                    )
                )

