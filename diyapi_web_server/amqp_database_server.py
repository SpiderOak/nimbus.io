# -*- coding: utf-8 -*-
"""
amqp_database_server.py

A class that represents a database server in the system.
"""
import logging

from diyapi_web_server.amqp_process import AMQPProcess

from diyapi_web_server.exceptions import (
    DatabaseServerDownError,
    ListmatchFailedError,
    StatFailedError,
)

from messages.database_listmatch import DatabaseListMatch
from messages.space_usage import SpaceUsage
from messages.stat import Stat


class AMQPDatabaseServer(AMQPProcess):
    _downerror_class = DatabaseServerDownError

    def listmatch(
        self,
        request_id,
        avatar_id,
        prefix
    ):
        message = DatabaseListMatch(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            prefix
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'prefix = %r' % (
                message.__class__.__name__,
                message.request_id,
                prefix,
            ))
        reply = self._send(message, ListmatchFailedError)
        return reply.key_list

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
        reply = self._send(message, ListmatchFailedError)

    def stat(
        self,
        request_id,
        avatar_id,
        path
    ):
        message = Stat(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            path
        )
        self.log.debug(
            '%s: '
            'request_id = %s, '
            'path = %r' % (
                message.__class__.__name__,
                message.request_id,
                path
            ))
        reply = self._send(message, StatFailedError)
        return dict(
            timestamp=reply.timestamp,
            total_size=reply.total_size,
            file_adler=reply.file_adler,
            file_md5=reply.file_md5,
            userid=reply.userid,
            groupid=reply.groupid,
            permissions=reply.permissions,
        )
