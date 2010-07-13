# -*- coding: utf-8 -*-
"""
amqp_data_writer.py

A class that represents a data writer in the system.
"""
import logging

import gevent
from gevent.event import AsyncResult

from diyapi_web_server.exceptions import (
    DataWriterDownError,
    ArchiveFailedError,
    HandoffFailedError,
    StartHandoff,
)

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_final import ArchiveKeyFinal
from messages.hinted_handoff import HintedHandoff


class AMQPDataWriter(object):
    def __init__(self, amqp_handler, exchange):
        self.log = logging.getLogger('AMQPDataWriter(%r)' % (exchange,))
        self.amqp_handler = amqp_handler
        self.exchange = exchange
        self.is_down = False

    def __hash__(self):
        return hash(self.exchange)

    def __eq__(self, other):
        if not isinstance(other, AMQPDataWriter):
            return False
        return self.exchange == other.exchange

    def __ne__(self, other):
        return not (self == other)

    def mark_down(self):
        self.log.debug('mark_down')
        self.is_down = True

    def _send(self, message, error_class):
        if self.is_down and not force:
            raise DataWriterDownError()
        reply = self.amqp_handler.send_message(
            message, self.exchange).get()
        if reply.error:
            self.log.error(
                '%s: '
                'request_id = %s, '
                'result = %d, '
                'error_message = %r' % (
                    reply.__class__.__name__,
                    reply.request_id,
                    reply.result,
                    reply.error_message,
                ))
            raise error_class(reply.result, reply.error_message)
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                reply.__class__.__name__,
                reply.request_id,
            ))
        return reply

    def archive_key_entire(
        self,
        request_id,
        avatar_id,
        timestamp,
        key,
        version_number,
        segment_number,
        file_adler32,
        file_md5,
        segment_adler32,
        segment_md5,
        segment
    ):
        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            version_number,
            segment_number,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'segment_number = %d' % (
                message.__class__.__name__,
                message.request_id,
                segment_number,
            ))
        try:
            reply = self._send(message, ArchiveFailedError)
        except StartHandoff, handoff:
            results = []
            for writer in handoff.to:
                result = AsyncResult()
                gevent.spawn(
                    writer._send,
                    message,
                    ArchiveFailedError
                ).link(result)
                results.append(result)
            return sum(result.get().previous_size for result in results)
        else:
            return reply.previous_size

    def archive_key_start(
        self,
        request_id,
        avatar_id,
        timestamp,
        sequence_number,
        key,
        version_number,
        segment_number,
        segment
    ):
        message = ArchiveKeyStart(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            sequence_number,
            key,
            version_number,
            segment_number,
            len(segment),
            segment
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'segment_number = %d' % (
                message.__class__.__name__,
                message.request_id,
                segment_number,
            ))
        try:
            reply = self._send(message, ArchiveFailedError)
        except StartHandoff, handoff:
            tasks = [gevent.spawn(writer._send, message, ArchiveFailedError)
                     for writer in handoff.to]
            gevent.joinall(tasks, raise_error=True)

    def archive_key_next(
        self,
        request_id,
        sequence_number,
        segment
    ):
        message = ArchiveKeyNext(
            request_id,
            sequence_number,
            segment
        )
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                message.__class__.__name__,
                message.request_id,
            ))
        reply = self._send(message, ArchiveFailedError)
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                reply.__class__.__name__,
                reply.request_id,
            ))

    def archive_key_final(
        self,
        request_id,
        sequence_number,
        file_size,
        file_adler32,
        file_md5,
        segment_adler32,
        segment_md5,
        segment
    ):
        message = ArchiveKeyFinal(
            request_id,
            sequence_number,
            file_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                message.__class__.__name__,
                message.request_id,
            ))
        reply = self._send(message, ArchiveFailedError)
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'previous_size = %r' % (
                reply.__class__.__name__,
                reply.request_id,
                reply.previous_size,
            ))
        return reply.previous_size

    def hinted_handoff(
        self,
        request_id,
        avatar_id,
        timestamp,
        key,
        version_number,
        segment_number,
        dest_exchange
    ):
        message = HintedHandoff(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            version_number,
            segment_number,
            dest_exchange,
        )
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                message.__class__.__name__,
                message.request_id,
            ))
        reply = self._send(message, HandoffFailedError)
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'previous_size = %r' % (
                reply.__class__.__name__,
                reply.request_id,
                reply.previous_size,
            ))
        return reply.previous_size
