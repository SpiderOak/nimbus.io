# -*- coding: utf-8 -*-
"""
amqp_retriever.py

A class that retrieves data from the nodes.
"""
import logging
import uuid

import gevent

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_start_reply import RetrieveKeyStartReply

from diyapi_web_server.exceptions import *


class AMQPRetriever(object):
    """Retrieves data from the nodes."""
    def __init__(self, amqp_handler, exchange_manager,
                 avatar_id, key, num_segments, segments_needed):
        self.log = logging.getLogger(
            'AMQPRetriever(avatar_id=%d, key=%r)' % (
                avatar_id, key))
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = 0
        self.num_segments = num_segments
        self.segments_needed = segments_needed
        self._segment_request_ids = {}
        self.pending = {}
        self.sequence_number = 0
        self.n_slices = 1
        self.result = None

    def _wait_for_reply(self, segment_number, reply_queue):
        try:
            while True:
                reply = reply_queue.get()
                if getattr(reply, 'sequence', 0) == self.sequence_number:
                    break
                self.log.debug(
                    '%s: segment_number = %d '
                    'out of sequence (was %d, expecting %d)' % (
                        reply.__class__.__name__,
                        segment_number,
                        getattr(reply, 'sequence', 0),
                        self.sequence_number,
                    ))
        except gevent.GreenletExit:
            return
        else:
            self.log.debug(
                '%s: segment_number = %d' % (
                    reply.__class__.__name__,
                    segment_number,
                ))
            if isinstance(reply, RetrieveKeyStartReply):
                self.n_slices = reply.segment_count
                self.slice_size = reply.segment_size
            if len(self.result) < self.segments_needed:
                self.result[segment_number] = reply.data_content
        finally:
            del self.pending[segment_number]
        if len(self.result) >= self.segments_needed:
            self.cancel()

    def cancel(self):
        self.log.debug('cancelling')
        gevent.killall(self.pending.values(), block=True)

    def _make_request(self, segment_number):
        if self.sequence_number == 0:
            self._segment_request_ids[segment_number] = uuid.uuid1().hex
            return RetrieveKeyStart(
                self._segment_request_ids[segment_number],
                self.avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                self.key,
                self.version_number,
                segment_number
            )
        elif self.sequence_number < self.n_slices - 1:
            return RetrieveKeyNext(
                self._segment_request_ids[segment_number],
                self.sequence_number
            )
        else:
            return RetrieveKeyFinal(
                self._segment_request_ids[segment_number],
                self.sequence_number
            )

    def retrieve(self, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        while self.sequence_number < self.n_slices:
            self.result = {}
            for segment_number in xrange(1, self.num_segments + 1):
                message = self._make_request(segment_number)
                for exchange in self.exchange_manager[segment_number - 1]:
                    self.log.debug(
                        '%s to %r: '
                        'segment_number = %d' % (
                            message.__class__.__name__,
                            exchange,
                            segment_number,
                        ))
                    reply_queue = self.amqp_handler.send_message(
                        message, exchange)
                    self.pending[segment_number] = gevent.spawn(
                        self._wait_for_reply, segment_number, reply_queue)
            gevent.joinall(self.pending.values(), timeout, True)
            if self.pending:
                self.cancel()
                raise RetrieveFailedError(
                    'expected %d segments, only got %d (sequence = %d)' % (
                        self.segments_needed,
                        len(self.result),
                        self.sequence_number))
            yield self.result
            self.sequence_number += 1
