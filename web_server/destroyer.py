# -*- coding: utf-8 -*-
"""
destroyer.py

A class that performs a destroy query on all data writers.
"""
import logging
import uuid

import gevent
import gevent.pool

from web_server.exceptions import (
    AlreadyInProgress,
    DestroyFailedError,
)

from web_server.local_database_util import current_status_of_key

class Destroyer(object):
    """Performs a destroy query on all data writers."""
    def __init__(
        self, 
        node_local_connection,
        data_writers,
        collection_id, 
        key,
        timestamp        
    ):
        self.log = logging.getLogger('Destroyer')
        self.log.info('collection_id=%d, key=%r' % (collection_id, key, ))
        self._node_local_connection = node_local_connection
        self.data_writers = data_writers
        self.collection_id = collection_id
        self.key = key
        self.timestamp = timestamp
        self._pending = gevent.pool.Group()
        self._done = []

    def _join(self, timeout):
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

    def _spawn(self, segment_num, data_writer, run, *args):
        method_name = run.__name__
        task = self._pending.spawn(run, *args)
        task.rawlink(self._done_link)
        task.segment_num = segment_num
        task.data_writer = data_writer
        task.method_name = method_name
        return task

    def destroy(self, timeout=None):
        if self._pending:
            raise AlreadyInProgress()

        # TODO: find a non-blocking way to do this
        conjoined_row, segment_rows = current_status_of_key(
            self._node_local_connection , self.collection_id, self.key
        )

        conjoined_identifier = (
            None if conjoined_row is None \
            else uuid.UUID(bytes=conjoined_row.identifier)
        )

        file_size = sum([row.file_size for row in segment_rows])

        for i, data_writer in enumerate(self.data_writers):
            segment_num = i + 1
            self._spawn(
                segment_num,
                data_writer,
                data_writer.destroy_key,
                self.collection_id,
                self.key,
                conjoined_identifier,
                self.timestamp,
                segment_num
            )
        self._join(timeout)
        self._done = []

        return file_size

