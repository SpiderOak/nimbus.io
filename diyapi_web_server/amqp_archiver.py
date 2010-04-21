# -*- coding: utf-8 -*-
"""
amqp_archiver.py

A class that sends data segments via AMQP to write processes on nodes.
"""
import os
import hashlib
import zlib
import uuid

import gevent

from messages.archive_key_entire import ArchiveKeyEntire


class AMQPArchiver(object):
    """Sends data segments via AMQP to write processes on nodes."""

    def __init__(self, amqp_handler, exchange_manager):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager

    def archive_entire(self, avatar_id, key, segments, timestamp,
                       timeout=None):
        replies = []
        for segment_number, segment in enumerate(segments):
            segment_number += 1
            request_id = uuid.uuid1().hex
            file_adler32 = 0
            file_md5 = ""
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            message = ArchiveKeyEntire(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                key,
                0, # version number
                segment_number,
                file_adler32,
                file_md5,
                segment_adler32,
                segment_md5,
                segment
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                reply_queue = self.amqp_handler.send_message(message, exchange)
                replies.append((message, gevent.spawn(reply_queue.get)))
        gevent.joinall([reply for (message, reply) in replies],
                       timeout=timeout)
        # TODO: do handoff when nodes are down
        assert all(reply.ready() for (message, reply) in replies)
        return sum(reply.value.previous_size for (message, reply) in replies)
