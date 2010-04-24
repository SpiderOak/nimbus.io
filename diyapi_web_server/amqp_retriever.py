# -*- coding: utf-8 -*-
"""
amqp_retriever.py

A class that retrieves data from the nodes.
"""
import uuid

import gevent

from messages.retrieve_key_start import RetrieveKeyStart


class AMQPRetriever(object):
    """Retrieves data from the nodes."""

    def __init__(self, amqp_handler, exchange_manager):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager

    def retrieve(self, avatar_id, key, timeout=None):
        num_segments = self.exchange_manager.num_exchanges
        replies = []
        for segment_number in xrange(1, num_segments + 1):
            request_id = uuid.uuid1().hex
            message = RetrieveKeyStart(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                key,
                0, # version_number
                segment_number
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                reply_queue = self.amqp_handler.send_message(message, exchange)
                replies.append((message, gevent.spawn(reply_queue.get)))

        # TODO: move on once we have enough segments
        gevent.joinall([reply for (message, reply) in replies],
                       timeout=timeout)
        # TODO: handle unresponsive nodes
        assert all(reply.ready() for (message, reply) in replies)

        reply_contents = {}
        for message, reply in replies:
            reply = reply.value
            segment = reply.data_content
            reply_contents[reply.segment_number] = segment
            # TODO: handle large files with multiple slices

        return reply_contents
