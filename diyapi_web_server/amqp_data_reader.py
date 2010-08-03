# -*- coding: utf-8 -*-
"""
amqp_data_reader.py

A class that represents a data reader in the system.
"""
import logging

from diyapi_web_server.amqp_process import AMQPProcess

from diyapi_web_server.exceptions import (
    DataReaderDownError,
    RetrieveFailedError,
    ListmatchFailedError,
    StatFailedError,
)

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal


class AMQPDataReader(AMQPProcess):
    _downerror_class = DataReaderDownError

    def retrieve_key_start(
        self,
        request_id,
        avatar_id,
        key,
        version_number,
        segment_number
    ):
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            key,
            version_number,
            segment_number
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'segment_number = %d' % (
                message.__class__.__name__,
                message.request_id,
                segment_number,
            ))
        reply = self._send(message, RetrieveFailedError)
        return reply

    def retrieve_key_next(
        self,
        request_id,
        sequence_number
    ):
        message = RetrieveKeyNext(
            request_id,
            sequence_number
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'sequence_number = %d' % (
                message.__class__.__name__,
                message.request_id,
                sequence_number,
            ))
        reply = self._send(message, RetrieveFailedError)
        return reply.data_content

    def retrieve_key_final(
        self,
        request_id,
        sequence_number
    ):
        message = RetrieveKeyFinal(
            request_id,
            sequence_number
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'sequence_number = %d' % (
                message.__class__.__name__,
                message.request_id,
                sequence_number,
            ))
        reply = self._send(message, RetrieveFailedError)
        return reply.data_content
