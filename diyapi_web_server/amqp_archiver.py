# -*- coding: utf-8 -*-
"""
amqp_archiver.py

A class that sends data segments via AMQP to write processes on nodes.
"""
import logging
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
                 avatar_id, key, timestamp):
        self.log = logging.getLogger(
            'AMQPArchiver(avatar_id=%d, key=%r)' % (
                avatar_id, key))
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = 0
        self.timestamp = timestamp
        self.pending = {}
        self.result = None

    def _do_handoff(self, message, exchange):
        # TODO: fix this code smell
        exchange_num = self.exchange_manager.exchanges.index(exchange)
        self.exchange_manager.mark_down(exchange_num)
        replies = []
        for exchange in self.exchange_manager[exchange_num]:
            self.log.debug(
                'handoff to %r: '
                'segment_number = %d' % (
                    exchange,
                    message.segment_number,
                ))
            reply_queue = self.amqp_handler.send_message(message, exchange)
            replies.append(gevent.spawn(reply_queue.get))
        gevent.joinall(replies)
        return sum(reply.value.previous_size for reply in replies)

    def _wait_for_reply(self, message, exchange, reply_queue):
        try:
            result = reply_queue.get().previous_size
            self.log.debug(
                'reply from %r: '
                'segment_number = %d: '
                'previous_size = %r' % (
                    exchange,
                    message.segment_number,
                    result,
                ))
            self.result += result
        except StartHandoff:
            self.result += self._do_handoff(message, exchange)
        del self.pending[message.segment_number]

    def start_handoff(self, timeout=None):
        self.log.info('starting handoff')
        gevent.killall(self.pending.values(), StartHandoff, True, timeout)

    def archive_entire(self, file_adler32, file_md5, segments, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        self.log.info('archive_entire')
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
                file_adler32,
                file_md5,
                segment_adler32,
                segment_md5,
                segment
            )
            # TODO: handle starting up in handoff mode better
            # _wait_for_reply will remove from pending when we get the first
            # message back
            for exchange in self.exchange_manager[i]:
                self.log.debug(
                    'archive_entire to %r: '
                    'segment_number = %d' % (
                        exchange,
                        message.segment_number,
                    ))
                reply_queue = self.amqp_handler.send_message(message, exchange)
                self.pending[message.segment_number] = gevent.spawn(
                    self._wait_for_reply, message, exchange, reply_queue)
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            self.start_handoff(timeout)
        if self.pending:
            raise HandoffFailedError()
        return self.result
