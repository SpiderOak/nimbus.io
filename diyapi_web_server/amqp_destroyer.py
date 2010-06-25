# -*- coding: utf-8 -*-
"""
amqp_destroyer.py

A class that performs a destroy query on all nodes.
"""
import logging
import uuid

import gevent

from messages.destroy_key import DestroyKey

from diyapi_web_server.exceptions import AlreadyInProgress, DestroyFailedError

class AMQPDestroyer(object):
    """Performs a destroy query on all nodes."""
    def __init__(self, amqp_handler, exchange_manager):
        self.log = logging.getLogger('AMQPDestroyer()')
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.pending = {}
        self.result = None

    def _wait_for_reply(self, exchange, reply_queue):
        reply = reply_queue.get()
        result = reply.total_size
        self.log.debug(
            '%s from %r: '
            'total_size = %r' % (
                reply.__class__.__name__,
                exchange,
                result,
            ))
        if self.result is None or result < self.result:
            self.result = result
        del self.pending[exchange]

    def destroy(self, avatar_id, key, timestamp, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        self.result = None
        for i in xrange(len(self.exchange_manager)):
            request_id = uuid.uuid1().hex
            message = DestroyKey(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                key,
                i + 1,   # segment_number
                0        # version number
            )
            exchange = self.exchange_manager[i]
            reply_queue = self.amqp_handler.send_message(message, exchange)
            self.pending[exchange] = gevent.spawn(
                self._wait_for_reply, exchange, reply_queue)
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            raise DestroyFailedError()
        return self.result
