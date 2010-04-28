# -*- coding: utf-8 -*-
"""
amqp_retriever.py

A class that retrieves data from the nodes.
"""
import logging
import uuid

import gevent

from messages.retrieve_key_start import RetrieveKeyStart


class AMQPRetriever(object):
    """Retrieves data from the nodes."""
    def __init__(self, amqp_handler, exchange_manager,
                 avatar_id, key, num_segments):
        self.log = logging.getLogger(
            'AMQPRetriever(avatar_id=%d, key=%r)' % (
                avatar_id, key))
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = 0
        self.num_segments = num_segments
        self.pending = {}
        self.result = None

    def _wait_for_reply(self, message, exchange, reply_queue):
        try:
            segment = reply_queue.get().data_content
            self.log.debug(
                'reply from %r: '
                'segment_number = %d' % (
                    exchange,
                    message.segment_number,
                ))
            if len(self.result) < self.num_segments:
                self.result[message.segment_number] = segment
        except gevent.GreenletExit:
            return
        finally:
            del self.pending[message.segment_number]
        if len(self.result) >= self.num_segments:
            self.cancel()

    def cancel(self):
        self.log.debug('cancelling')
        gevent.killall(self.pending.values(), block=True)

    def retrieve(self, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        self.log.info('retrieve')
        self.result = {}
        for segment_number in xrange(1, self.num_segments + 1):
            request_id = uuid.uuid1().hex
            message = RetrieveKeyStart(
                request_id,
                self.avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                self.key,
                self.version_number,
                segment_number
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                self.log.debug(
                    'retrieve from %r: '
                    'segment_number = %d' % (
                        exchange,
                        message.segment_number,
                    ))
                reply_queue = self.amqp_handler.send_message(message, exchange)
                self.pending[message.segment_number] = gevent.spawn(
                    self._wait_for_reply, message, exchange, reply_queue)
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            self.cancel()
            if len(self.result) < self.num_segments:
                raise RetrieveFailedError(
                    'expected %d segments, only got %d' % (
                        self.num_segments, len(self.result)))
        # TODO: can this happen? think of a test to check
        while len(self.result) > self.num_segments:
            self.result.popitem()
        return self.result
