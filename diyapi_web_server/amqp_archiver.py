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

    def __init__(self, sender):
        self.sender = sender

    def archive_entire(self, avatar_id, key, segments, timestamp):
        replies = []
        for i, segment in enumerate(segments):
            request_id = uuid.uuid1().hex
            file_adler32 = 0
            file_md5 = ""
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            message = ArchiveKeyEntire(
                request_id,
                avatar_id,
                self.sender.reply_exchange,
                self.sender.reply_queue,
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
            reply_queue = self.sender.send_to_exchange(i, message)
            replies.append(gevent.spawn(reply_queue.get))
        gevent.joinall(replies, raise_error=True)
        return sum(reply.value.previous_size for reply in replies)
