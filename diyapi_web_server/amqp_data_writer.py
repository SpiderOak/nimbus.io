# -*- coding: utf-8 -*-
"""
amqp_data_writer.py

A class that represents a data writer in the system.
"""
import os
import logging

import gevent

from diyapi_web_server.amqp_process import AMQPProcess

from diyapi_web_server.exceptions import (
    DataWriterDownError,
    ArchiveFailedError,
    DestroyFailedError,
    HandoffFailedError,
    StartHandoff,
)

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_final import ArchiveKeyFinal
from messages.destroy_key import DestroyKey
from messages.hinted_handoff import HintedHandoff
from messages.process_status import ProcessStatus

_HEARTBEAT_INTERVAL = float(
    os.environ.get("DIYAPI_DATA_WRITER_HEARTBEAT", "60.0")
)


class AMQPDataWriter(AMQPProcess):
    _downerror_class = DataWriterDownError

    def __init__(self, amqp_handler, exchange):
        super(AMQPDataWriter, self).__init__(amqp_handler, exchange)
        self.heartbeat_interval = _HEARTBEAT_INTERVAL * 2
        self._heartbeat_timeout = gevent.spawn_later(
            self.heartbeat_interval,
            self.mark_down
        )
        self._heartbeat_subscription = self._heartbeat
        amqp_handler.subscribe(ProcessStatus, self._heartbeat_subscription)

    def _heartbeat(self, message):
        self._heartbeat_timeout.kill()
        self._heartbeat_timeout = gevent.spawn_later(
            self.heartbeat_interval,
            self.mark_down
        )
        if message.status == ProcessStatus.status_shutdown:
            self.mark_down()
        else:
            self.mark_up()

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
        reply = self._send(message, ArchiveFailedError)
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
        reply = self._send(message, ArchiveFailedError)

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
            'previous_size = %r' % (
                reply.previous_size,
            ))
        return reply.previous_size

    def destroy_key(
        self,
        request_id,
        avatar_id,
        timestamp,
        key,
        segment_number,
        version_number
    ):
        message = DestroyKey(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            segment_number,
            version_number
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'segment_number = %d' % (
                message.__class__.__name__,
                message.request_id,
                segment_number,
            ))
        reply = self._send(message, DestroyFailedError)
        return reply.total_size

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
            'previous_size = %r' % (
                reply.previous_size,
            ))
        return reply.previous_size
