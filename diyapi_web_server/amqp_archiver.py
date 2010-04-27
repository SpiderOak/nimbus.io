# -*- coding: utf-8 -*-
"""
amqp_archiver.py

A class that sends data segments via AMQP to write processes on nodes.
"""
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

    def archive_entire(self, avatar_id, key,
                       file_adler32, file_md5,
                       segments, timestamp):
        replies = []
        for i, segment in enumerate(segments):
            request_id = uuid.uuid1().hex
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
                i + 1, # segment number
                file_adler32,
                file_md5,
                segment_adler32,
                segment_md5,
                segment
            )
            for exchange in self.exchange_manager[i]:
                reply_queue = self.amqp_handler.send_message(message, exchange)
                replies.append(gevent.spawn(reply_queue.get))
        gevent.joinall(replies, raise_error=True)
        if not all(reply.ready() for reply in replies):
            # TODO: raise a specific error
            raise RuntimeError()
        return sum(reply.value.previous_size for reply in replies)
