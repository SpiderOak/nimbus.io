# -*- coding: utf-8 -*-
"""
amqp_destroyer.py

A class that performs a destroy query on all nodes.
"""
import logging
import uuid

import gevent

from messages.destroy_key import DestroyKey

from diyapi_web_server.exceptions import *


class StartHandoff(Exception):
    pass


class AMQPDestroyer(object):
    """Performs a destroy query on all nodes."""
    def __init__(self, amqp_handler, exchange_manager):
        self.log = logging.getLogger('AMQPDestroyer()')
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.pending = {}
        self.result = None

    def _do_handoff(self, message, exchange):
        exchange_num = self.exchange_manager.index(exchange)
        self.exchange_manager.mark_down(exchange_num)
        replies = []
        for exchange in self.exchange_manager.handoff_exchanges(exchange_num):
            self.log.debug(
                '%s to %r (handoff)' % (
                    message.__class__.__name__,
                    exchange,
                ))
            reply_queue = self.amqp_handler.send_message(message, exchange)
            replies.append(gevent.spawn(reply_queue.get))
        gevent.joinall(replies)
        result = min(reply.value.total_size for reply in replies)
        if self.result is None or result < self.result:
            self.result = result

    def _wait_for_reply(self, message, exchange, reply_queue):
        try:
            reply = reply_queue.get()
        except StartHandoff:
            self._do_handoff(message, exchange)
        else:
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

    def start_handoff(self, timeout=None):
        self.log.info('starting handoff')
        try:
            gevent.killall(self.pending.values(), StartHandoff, True, timeout)
        except gevent.Timeout:
            pass

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
                0,
                i + 1   # segment_number
            )
            exchange = self.exchange_manager[i]
            reply_queue = self.amqp_handler.send_message(message, exchange)
            self.pending[exchange] = gevent.spawn(
                self._wait_for_reply, message, exchange, reply_queue)
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            self.start_handoff(timeout)
        if self.pending:
            raise HandoffFailedError()
        return self.result
