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

from diyapi_web_server.exceptions import *


class StartHandoff(Exception):
    pass


class AMQPArchiver(object):
    """Sends data segments via AMQP to write processes on nodes."""
    def __init__(self, amqp_handler, exchange_manager,
                 avatar_id, key, file_adler32, file_md5, timestamp):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = 0
        self.file_adler32 = file_adler32
        self.file_md5 = file_md5
        self.timestamp = timestamp
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

    def archive_entire(self, segments, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        self.result = 0
        for i, segment in enumerate(segments):
            request_id = uuid.uuid1().hex
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            message = ArchiveKeyEntire(
                request_id,
                self.avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                self.timestamp,
                self.key,
                self.version_number,
                i + 1, # segment number
                self.file_adler32,
                self.file_md5,
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
        if self.pending:
            raise HandoffFailedError()
        return self.result
