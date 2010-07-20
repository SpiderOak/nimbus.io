# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import logging
import uuid

import gevent
from gevent.pool import GreenletSet

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_start_reply import RetrieveKeyStartReply

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
        self.n_slices = self._done[0].value.segment_count
        yield dict((task.segment_number, task.value.data_content)
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

    #def _wait_for_reply(self, segment_number, reply_queue):
    #    try:
    #        while True:
    #            reply = reply_queue.get()
    #            if getattr(reply, 'sequence', 0) == self.sequence_number:
    #                break
    #            self.log.debug(
    #                '%s: segment_number = %d '
    #                'out of sequence (was %d, expecting %d)' % (
    #                    reply.__class__.__name__,
    #                    segment_number,
    #                    getattr(reply, 'sequence', 0),
    #                    self.sequence_number,
    #                ))
    #    except gevent.GreenletExit:
    #        return
    #    else:
    #        self.log.debug(
    #            '%s: segment_number = %d, result = %d' % (
    #                reply.__class__.__name__,
    #                segment_number,
    #                reply.result,
    #            ))
    #        if reply.result != reply.successful:
    #            return
    #        if isinstance(reply, RetrieveKeyStartReply):
    #            self.n_slices = reply.segment_count
    #            self.slice_size = reply.segment_size
    #        if len(self.result) < self.segments_needed:
    #            self.result[segment_number] = reply.data_content
    #    finally:
    #        del self.pending[segment_number]
    #    if len(self.result) >= self.segments_needed:
    #        self.cancel()

    #def cancel(self):
    #    self.log.debug('cancelling')
    #    gevent.killall(self.pending.values(), block=True)

    #def _make_request(self, segment_number):
    #    if self.sequence_number == 0:
    #        self._segment_request_ids[segment_number] = uuid.uuid1().hex
    #        return RetrieveKeyStart(
    #            self._segment_request_ids[segment_number],
    #            self.avatar_id,
    #            self.amqp_handler.exchange,
    #            self.amqp_handler.queue_name,
    #            self.key,
    #            self.version_number,
    #            segment_number
    #        )
    #    elif self.sequence_number < self.n_slices - 1:
    #        return RetrieveKeyNext(
    #            self._segment_request_ids[segment_number],
    #            self.sequence_number
    #        )
    #    else:
    #        return RetrieveKeyFinal(
    #            self._segment_request_ids[segment_number],
    #            self.sequence_number
    #        )

    #def retrieve(self, timeout=None):
    #    if self.pending:
    #        raise AlreadyInProgress()
    #    while self.sequence_number < self.n_slices:
    #        self.result = {}
    #        for segment_number in xrange(1, self.num_segments + 1):
    #            message = self._make_request(segment_number)
    #            exchange = self.exchange_manager[segment_number - 1]
    #            self.log.debug(
    #                '%s to %r: '
    #                'segment_number = %d' % (
    #                    message.__class__.__name__,
    #                    exchange,
    #                    segment_number,
    #                ))
    #            reply_queue = self.amqp_handler.send_message(
    #                message, exchange)
    #            self.pending[segment_number] = gevent.spawn(
    #                self._wait_for_reply, segment_number, reply_queue)
    #        gevent.joinall(self.pending.values(), timeout, True)
    #        if self.pending:
    #            self.cancel()
    #        if len(self.result) < self.segments_needed:
    #            raise RetrieveFailedError(
    #                'expected %d segments, only got %d (sequence = %d)' % (
    #                    self.segments_needed,
    #                    len(self.result),
    #                    self.sequence_number))
    #        yield self.result
    #        self.sequence_number += 1
