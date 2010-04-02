# -*- coding: utf-8 -*-
"""
amqp_data_archiver.py

A class that sends data via messaging to write processes on nodes.
"""
import hashlib
import zlib
import uuid
import time

from messages.archive_key_entire import ArchiveKeyEntire


class AMQPDataArchiver(object):
    """Sends data via messaging to write processes on nodes."""

    def __init__(self, amqp_handler):
        self.amqp_handler = amqp_handler

    def archive_entire(self, avatar_id, key, content, timestamp=time.time()):
        request_id = uuid.uuid1().hex
        adler32 = zlib.adler32(content)
        md5 = hashlib.md5(content).digest()
        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            key,
            timestamp,
            3,  # TODO: we're only sending to the local node at this point
            adler32,
            md5,
            content
        )
        replies = self.amqp_handler.send_message(message)
        reply = replies.get()
        return reply.previous_size
