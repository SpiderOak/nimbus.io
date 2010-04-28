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


class StartHandoff(Exception):
    pass


class AMQPArchiver(object):
    """Sends data segments via AMQP to write processes on nodes."""

    def __init__(self, amqp_handler, exchange_manager):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.pending = {}
        self.result = None

    def _do_handoff(self, message, exchange):
        # TODO: fix this code smell
        exchange_num = self.exchange_manager.exchanges.index(exchange)
        self.exchange_manager.mark_down(exchange_num)
        replies = []
        for exchange in self.exchange_manager[exchange_num]:
            reply_queue = self.amqp_handler.send_message(message, exchange)
            replies.append(gevent.spawn(reply_queue.get))
        gevent.joinall(replies)
        return sum(reply.value.previous_size for reply in replies)

    def _wait_for_reply(self, message, exchange, reply_queue):
        try:
            self.result += reply_queue.get().previous_size
        except StartHandoff:
            self.result += self._do_handoff(message, exchange)
        del self.pending[message.segment_number]

    def archive_entire(self, avatar_id, key,
                       file_adler32, file_md5,
                       segments, timestamp, timeout=None):
        if self.pending:
            # TODO: raise a specific error
            raise RuntimeError()
        self.result = 0
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
                self.pending[message.segment_number] = gevent.spawn(
                    self._wait_for_reply, message, exchange, reply_queue)
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            gevent.killall(self.pending.values(), StartHandoff, True, timeout)
        assert not self.pending, self.pending
        return self.result
