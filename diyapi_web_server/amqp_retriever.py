# -*- coding: utf-8 -*-
"""
amqp_retriever.py

A class that retrieves data from the nodes.
"""
import uuid

from messages.database_key_list import DatabaseKeyList
from messages.retrieve_key_start import RetrieveKeyStart


class AMQPRetriever(object):
    """Retrieves data from the nodes."""

    def __init__(self, amqp_handler, exchanges, min_segments):
        self.amqp_handler = amqp_handler
        self.exchanges = exchanges
        self.min_segments = min_segments

    def _retrieve(self, avatar_id, key):
        key_lists = self._get_key_lists(avatar_id, key)
        segments = self._get_segment_data(avatar_id, key, key_lists)
        return segments

    def _get_key_lists(self, avatar_id, key):
        reply_queues = {}
        for exchange in self.exchanges:
            request_id = uuid.uuid1().hex
            message = DatabaseKeyList(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                key
            )
            reply_queues[exchange] = self.amqp_handler.send_message(
                message, exchange)

        reply_contents = {}
        for exchange, reply_queue in reply_queues.iteritems():
            reply = reply_queue.get()
            for content in reply.content_list:
                reply_contents[content.segment_number] = exchange, content
            # TODO: handle large files with multiple slices

        if len(reply_contents) < self.min_segments:
            # TODO: handle not enough segments
            raise RuntimeError()

        return reply_contents

    def _get_segment_data(self, avatar_id, key, key_lists):
        # TODO: retrieve segments
        return []

    def retrieve(self, avatar_id, key, timeout=None):
        task = gevent.spawn(self._retrieve(avatar_id, key))
        task.join(timeout=timeout)
        if not task.successful():
            task.kill()
            # TODO: raise a specific error here
            raise RuntimeError()
        return task.value
