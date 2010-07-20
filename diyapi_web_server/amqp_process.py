# -*- coding: utf-8 -*-
"""
amqp_process.py

A class that represents a process in the system.
"""
import logging


class AMQPProcess(object):
    def __init__(self, amqp_handler, exchange):
        self.log = logging.getLogger('%s(%r)' % (
            self.__class__.__name__,
            exchange
        ))
        self.amqp_handler = amqp_handler
        self.exchange = exchange
        self.is_down = False

    def __hash__(self):
        return hash(self.exchange)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.exchange == other.exchange

    def __ne__(self, other):
        return not (self == other)

    def mark_up(self):
        if self.is_down:
            self.log.debug('mark_up')
            self.is_down = False

    def mark_down(self):
        if not self.is_down:
            self.log.debug('mark_down')
            self.is_down = True

    def _send(self, message, error_class):
        if self.is_down:
            raise self._downerror_class()
        reply = self.amqp_handler.send_message(
            message, self.exchange).get()
        if reply.error:
            self.log.error(
                '%s: '
                'request_id = %s, '
                'result = %d, '
                'error_message = %r' % (
                    reply.__class__.__name__,
                    reply.request_id,
                    reply.result,
                    reply.error_message,
                ))
            raise error_class(reply.result, reply.error_message)
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                reply.__class__.__name__,
                reply.request_id,
            ))
        return reply
