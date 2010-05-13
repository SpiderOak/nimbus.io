# -*- coding: utf-8 -*-
"""
amqp_archiver.py

A class that sends data segments via AMQP to write processes on nodes.
"""
import logging
import hashlib
import zlib
import uuid
from collections import defaultdict

import gevent

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_final import ArchiveKeyFinal

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
        self._segment_request_ids = {}
        self._segment_adler32s = {}
        self._segment_md5s = defaultdict(hashlib.md5)
        self.sequence_number = 0
        self.result = None

    def _do_handoff(self, segment_number, message, exchange):
        # TODO: fix this code smell
        exchange_num = self.exchange_manager.exchanges.index(exchange)
        self.exchange_manager.mark_down(exchange_num)
        replies = []
        for exchange in self.exchange_manager[exchange_num]:
            self.log.debug(
                'handoff to %r: '
                'segment_number = %d' % (
                    exchange,
                    segment_number,
                ))
            reply_queue = self.amqp_handler.send_message(message, exchange)
            replies.append(gevent.spawn(reply_queue.get))
        gevent.joinall(replies)
        return sum(reply.value.previous_size for reply in replies)

    def _wait_for_reply(self, segment_number, message, exchange, reply_queue):
        try:
            reply = reply_queue.get()
        except StartHandoff:
            self.result += self._do_handoff(segment_number, message, exchange)
        else:
            try:
                result = reply.previous_size
                self.log.debug(
                    '%s from %r: '
                    'segment_number = %d: '
                    'previous_size = %r' % (
                        reply.__class__.__name__,
                        exchange,
                        segment_number,
                        result,
                    ))
                self.result += result
            except AttributeError:
                self.log.debug(
                    '%s from %r: '
                    'segment_number = %d' % (
                        reply.__class__.__name__,
                        exchange,
                        segment_number,
                    ))
        del self.pending[segment_number]

    def start_handoff(self, timeout=None):
        self.log.info('starting handoff')
        gevent.killall(self.pending.values(), StartHandoff, True, timeout)

    def archive_slice(self, segments, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_number = i + 1
            self._segment_adler32s[segment_number] = zlib.adler32(
                segment,
                self._segment_adler32s.get(segment_number, zlib.adler32(''))
            )
            self._segment_md5s[segment_number].update(segment)
            if self.sequence_number == 0:
                self._segment_request_ids[segment_number] = uuid.uuid1().hex
                message = ArchiveKeyStart(
                    self._segment_request_ids[segment_number],
                    self.avatar_id,
                    self.amqp_handler.exchange,
                    self.amqp_handler.queue_name,
                    self.timestamp,
                    self.sequence_number,
                    self.key,
                    self.version_number,
                    segment_number,
                    len(segment),
                    segment
                )
            else:
                message = ArchiveKeyNext(
                    self._segment_request_ids[segment_number],
                    self.sequence_number,
                    segment
                )
            # TODO: handle starting up in handoff mode better
            # _wait_for_reply will remove from pending when we get the first
            # message back
            for exchange in self.exchange_manager[i]:
                self.log.debug(
                    '%s to %r '
                    'segment_number = %d' % (
                        message.__class__.__name__,
                        exchange,
                        segment_number,
                    ))
                reply_queue = self.amqp_handler.send_message(message, exchange)
                self.pending[segment_number] = gevent.spawn(
                    self._wait_for_reply, segment_number,
                    message, exchange, reply_queue)
        self.sequence_number += 1
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            raise ArchiveFailedError()

    def archive_final(self, file_size, file_adler32, file_md5,
                      segments, timeout=None):
        if self.pending:
            raise AlreadyInProgress()
        self.result = 0
        for i, segment in enumerate(segments):
            segment_number = i + 1
            self._segment_adler32s[segment_number] = zlib.adler32(
                segment,
                self._segment_adler32s.get(segment_number, zlib.adler32(''))
            )
            self._segment_md5s[segment_number].update(segment)
            if self.sequence_number == 0:
                self._segment_request_ids[segment_number] = uuid.uuid1().hex
                message = ArchiveKeyEntire(
                    self._segment_request_ids[segment_number],
                    self.avatar_id,
                    self.amqp_handler.exchange,
                    self.amqp_handler.queue_name,
                    self.timestamp,
                    self.key,
                    self.version_number,
                    segment_number,
                    file_adler32,
                    file_md5,
                    self._segment_adler32s[segment_number],
                    self._segment_md5s[segment_number].digest(),
                    segment
                )
            else:
                message = ArchiveKeyFinal(
                    self._segment_request_ids[segment_number],
                    self.sequence_number,
                    file_size,
                    file_adler32,
                    file_md5,
                    self._segment_adler32s[segment_number],
                    self._segment_md5s[segment_number].digest(),
                    segment
                )
            # TODO: handle starting up in handoff mode better
            # _wait_for_reply will remove from pending when we get the first
            # message back
            for exchange in self.exchange_manager[i]:
                self.log.debug(
                    '%s to %r '
                    'segment_number = %d' % (
                        message.__class__.__name__,
                        exchange,
                        segment_number,
                    ))
                reply_queue = self.amqp_handler.send_message(message, exchange)
                self.pending[segment_number] = gevent.spawn(
                    self._wait_for_reply, segment_number,
                    message, exchange, reply_queue)
        gevent.joinall(self.pending.values(), timeout, True)
        if self.pending:
            self.start_handoff(timeout)
        if self.pending:
            raise HandoffFailedError()
        return self.result
