# -*- coding: utf-8 -*-
"""
amqp_archiver.py

A class that sends data segments via AMQP to write processes on nodes.
"""
import os
import hashlib
import zlib
import uuid
import time

import gevent

from messages.archive_key_entire import ArchiveKeyEntire

EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
REPLY_TIMEOUT = 10 # seconds


class AMQPArchiver(object):
    """Sends data segments via AMQP to write processes on nodes."""

    def __init__(self, amqp_handler):
        self.amqp_handler = amqp_handler
        self.exchanges = EXCHANGES

    def _exchanges_for_segment_number(self, segment_number):
        return [self.exchanges[segment_number]]

    def archive_entire(self, avatar_id, key, segments, timestamp=time.time()):
        replies = []
        for segment_number, segment in enumerate(segments):
            request_id = uuid.uuid1().hex
            adler32 = zlib.adler32(segment)
            md5 = hashlib.md5(segment).digest()
            message = ArchiveKeyEntire(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                key,
                timestamp,
                segment_number,
                adler32,
                md5,
                segment
            )
            for exchange in self._exchanges_for_segment_number(segment_number):
                reply_queue = self.amqp_handler.send_message(message, exchange)
                replies.append((message, gevent.spawn(reply_queue.get)))
        gevent.joinall([reply for (message, reply) in replies],
                       timeout=REPLY_TIMEOUT)
        # TODO: do handoff when nodes are down
        assert all(reply.ready() for (message, reply) in replies)
        return sum(reply.value.previous_size for (message, reply) in replies)
