# -*- coding: utf-8 -*-
"""
amqp_space_accounting_server.py

Sends space accounting messages.
"""
from diyapi_web_server.amqp_process import AMQPProcess

from diyapi_web_server.exceptions import (
    SpaceAccountingServerDownError,
    SpaceUsageFailedError,
)

from messages.space_accounting_detail import SpaceAccountingDetail
from messages.space_usage import SpaceUsage


class AMQPSpaceAccountingServer(AMQPProcess):
    _downerror_class = SpaceAccountingServerDownError

    def added(self, avatar_id, timestamp, bytes):
        if self.is_down:
            raise self._downerror_class()
        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_added,
            bytes
        )
        self.amqp_handler.send_message(message, self.exchange)

    def retrieved(self, avatar_id, timestamp, bytes):
        if self.is_down:
            raise self._downerror_class()
        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_retrieved,
            bytes
        )
        self.amqp_handler.send_message(message, self.exchange)

    def removed(self, avatar_id, timestamp, bytes):
        if self.is_down:
            raise self._downerror_class()
        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_removed,
            bytes
        )
        self.amqp_handler.send_message(message, self.exchange)

    def get_space_usage(
        self,
        request_id,
        avatar_id
    ):
        message = SpaceUsage(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name
        )
        self.log.debug(
            '%s: '
            'request_id = %s' % (
                message.__class__.__name__,
                message.request_id,
            ))
        reply = self._send(message, SpaceUsageFailedError)
        return {
            'bytes_added': reply.bytes_added,
            'bytes_removed': reply.bytes_removed,
            'bytes_retrieved': reply.bytes_retrieved,
        }
