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
from messages.hinted_handoff import HintedHandoff

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
        self.sequence_number = 0
        self._handoff_exchanges = {}
        self._request_ids = {}
        self._adler32s = {}
        self._md5s = defaultdict(hashlib.md5)
        self._pending = {}
        self._done = []
        self._result = None
        self._failed = False

    def _wait_for_reply(self, segment_number, message, exchange,
                        reply_queue, dest_exchange=None):
        try:
            reply = reply_queue.get()
        except StartHandoff:
            if (
                dest_exchange is not None or
                len(self._handoff_exchanges) >= 2 or
                len(self._done) < 2
            ):
                gevent.killall(self._pending.values())
                self._failed = True
                return
            exchange_num = self.exchange_manager.index(exchange)
            self.exchange_manager.mark_down(exchange_num)
            self._handoff_exchanges[segment_number] = self._done[:2]
            del self._done[:2]
            self._send_message(message, segment_number)
            return
        finally:
            del self._pending[segment_number, exchange]
        self._done.append(exchange)

        if reply.error:
            self.log.error(
                '%s from %r: '
                'segment_number = %d, '
                'result = %d, '
                'error_message = %r' % (
                    reply.__class__.__name__,
                    exchange,
                    segment_number,
                    reply.result,
                    reply.error_message,
                ))
            self._failed = True
            return

        if isinstance(message, (ArchiveKeyStart, ArchiveKeyNext)):
            self.log.debug(
                '%s from %r: '
                'segment_number = %d%s' % (
                    reply.__class__.__name__,
                    exchange,
                    segment_number,
                    ' (handoff)' if dest_exchange is not None else '',
                ))
        else:
            result = reply.previous_size
            self.log.debug(
                '%s from %r: '
                'segment_number = %d: '
                'previous_size = %r%s' % (
                    reply.__class__.__name__,
                    exchange,
                    segment_number,
                    result,
                    ' (handoff)' if dest_exchange is not None else '',
                ))
            self._result += result
            if dest_exchange is not None:
                handoff = gevent.spawn(self._send_hinted_handoff,
                                       segment_number, exchange, dest_exchange)
                self._pending[segment_number, exchange] = handoff
                try:
                    handoff.get()
                finally:
                    del self._pending[segment_number, exchange]

    def _send_hinted_handoff(self, segment_number, exchange, dest_exchange):
        message = HintedHandoff(
            self._request_ids[segment_number],
            self.avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            self.timestamp,
            self.key,
            self.version_number,
            segment_number,
            self.exchange_manager[segment_number - 1],
        )
        self.log.debug(
            '%s to %r '
            'segment_number = %d' % (
                message.__class__.__name__,
                exchange,
                segment_number,
            ))
        reply_queue = self.amqp_handler.send_message(message, exchange)
        reply = reply_queue.get()

        if reply.error:
            self.log.error(
                '%s from %r: '
                'segment_number = %d, '
                'result = %d, '
                'error_message = %r' % (
                    reply.__class__.__name__,
                    exchange,
                    segment_number,
                    reply.result,
                    reply.error_message,
                ))
            self._failed = True
            return

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
        self._result += result

    def start_handoff(self):
        self.log.info('starting handoff')
        gevent.killall(self._pending.values(), StartHandoff, True)

    def _send_message(self, message, segment_number):
        if segment_number in self._handoff_exchanges:
            exchanges = self._handoff_exchanges[segment_number]
            dest_exchange = self.exchange_manager[segment_number - 1]
        else:
            exchanges = [self.exchange_manager[segment_number - 1]]
            dest_exchange = None
        for exchange in exchanges:
            self.log.debug(
                '%s to %r '
                'segment_number = %d%s' % (
                    message.__class__.__name__,
                    exchange,
                    segment_number,
                    ' (handoff)' if dest_exchange is not None else '',
                ))
            reply_queue = self.amqp_handler.send_message(message, exchange)
            self._pending[segment_number, exchange] = gevent.spawn(
                self._wait_for_reply, segment_number, message,
                exchange, reply_queue, dest_exchange)

    def _join(self, timeout):
        gevent.joinall(self._pending.values(), timeout, True)
        if self.sequence_number == 0 and self._pending and not self._failed:
            self.start_handoff()
            gevent.joinall(self._pending.values(), timeout, True)
        if self._pending or self._failed:
            gevent.killall(self._pending.values())
            raise ArchiveFailedError()

    def archive_slice(self, segments, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_number = i + 1
            self._adler32s[segment_number] = zlib.adler32(
                segment,
                self._adler32s.get(segment_number, 1)
            )
            self._md5s[segment_number].update(segment)
            if self.sequence_number == 0:
                self._request_ids[segment_number] = uuid.uuid1().hex
                message = ArchiveKeyStart(
                    self._request_ids[segment_number],
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
                    self._request_ids[segment_number],
                    self.sequence_number,
                    segment
                )
            self._send_message(message, segment_number)
        self._join(timeout)
        self.sequence_number += 1

    def archive_final(self, file_size, file_adler32, file_md5,
                      segments, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        self._result = 0
        for i, segment in enumerate(segments):
            segment_number = i + 1
            self._adler32s[segment_number] = zlib.adler32(
                segment,
                self._adler32s.get(segment_number, 1)
            )
            self._md5s[segment_number].update(segment)
            if self.sequence_number == 0:
                self._request_ids[segment_number] = uuid.uuid1().hex
                message = ArchiveKeyEntire(
                    self._request_ids[segment_number],
                    self.avatar_id,
                    self.amqp_handler.exchange,
                    self.amqp_handler.queue_name,
                    self.timestamp,
                    self.key,
                    self.version_number,
                    segment_number,
                    file_adler32,
                    file_md5,
                    self._adler32s[segment_number],
                    self._md5s[segment_number].digest(),
                    segment
                )
            else:
                message = ArchiveKeyFinal(
                    self._request_ids[segment_number],
                    self.sequence_number,
                    file_size,
                    file_adler32,
                    file_md5,
                    self._adler32s[segment_number],
                    self._md5s[segment_number].digest(),
                    segment
                )
            self._send_message(message, segment_number)
        self._join(timeout)
        return self._result
