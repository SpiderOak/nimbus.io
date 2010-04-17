# -*- coding: utf-8 -*-
"""
amqp_destroyer.py

A class that performs a destroy query on all nodes.
"""
import uuid

import gevent

from messages.database_key_destroy import DatabaseKeyDestroy


class AMQPDestroyer(object):
    """Performs a destroy query on all nodes."""

    def __init__(self, amqp_handler, exchange_manager):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager

    def destroy(self, avatar_id, key, timestamp, timeout=None):
        replies = []
        for exchange in self.exchange_manager:
            request_id = uuid.uuid1().hex
            message = DatabaseKeyDestroy(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                key,
                0,
                0 # TODO: segment number?
            )
            reply_queue = self.amqp_handler.send_message(message, exchange)
            replies.append((message, gevent.spawn(reply_queue.get)))
        gevent.joinall([reply for (message, reply) in replies],
                       timeout=timeout)
        # TODO: do something when nodes are down
        assert all(reply.ready() for (message, reply) in replies)
        return min(reply.value.total_size for (message, reply) in replies)
