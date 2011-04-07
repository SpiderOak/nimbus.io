# -*- coding: utf-8 -*-
"""
data_writer.py

A class that represents a data writer in the system.
"""
from base64 import b64encode
import os
import logging

import gevent

from diyapi_web_server.exceptions import (
    DataWriterDownError,
    ArchiveFailedError,
    DestroyFailedError,
    HandoffFailedError,
    StartHandoff,
)

class DataWriter(object):

    def __init__(self, node_name, xreq_socket):
        self._log = logging.getLogger("DataWriter-%s" % (node_name, ))
        self._xreq_socket = xreq_socket

    def archive_key_entire(
        self,
        request_id,
        avatar_id,
        timestamp,
        key,
        version_number,
        segment_number,
        total_size,
        file_adler32,
        file_md5,
        segment_adler32,
        segment_md5,
        segment
    ):
        message = {
            "message-type"      : "archive-key-entire",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
            'key = %(key)r '
            'segment_number = %(segment-number)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])
        self._log.debug('previous_size = %(previous-size)r' % reply)
        return reply["previous-size"]

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
        message = {
            "message-type"      : "archive-key-start",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "sequence"          : sequence_number,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "segment-size"      : len(segment)
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
            'key = %(key)r '
            'segment_number = %(segment-number)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_next(
        self,
        request_id,
        sequence_number,
        segment
    ):
        message = {
            "message-type"      : "archive-key-next",
            "request-id"        : request_id,
            "sequence"          : sequence_number,
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%s(message-type): '
            'request_id = %(request-id)s '
            'sequence = %(sequence)s' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

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
        message = {
            "message-type"      : "archive-key-final",
            "request-id"        : request_id,
            "sequence"          : sequence_number,
            "total-size"        : file_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])
        self._log.debug('previous_size = %(previous-size)r' % reply)
        return reply["previous-size"]

    def destroy_key(
        self,
        request_id,
        avatar_id,
        timestamp,
        key,
        segment_number,
        version_number
    ):
        message = {
            "message-type"      : "destroy-key",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number,
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(message)
        self._log.debug(
            '%s(message-type): '
            'request_id = %(request-id)s, '
            'key = %(key)r '
            'segment_number = %(segment-number)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise DestroyFailedError(reply["error-message"])
        self._log.debug('total_size = %(total-size)r' % reply)
        return reply["total-size"]

#    def hinted_handoff(
#        self,
#        request_id,
#        avatar_id,
#        timestamp,
#        key,
#        version_number,
#        segment_number,
#        dest_exchange
#    ):
#        message = HintedHandoff(
#            request_id,
#            avatar_id,
#            self.amqp_handler.exchange,
#            self.amqp_handler.queue_name,
#            timestamp,
#            key,
#            version_number,
#            segment_number,
#            dest_exchange,
#        )
#        self._log.debug(
#            '%s: '
#            'request_id = %s' % (
#                message.__class__.__name__,
#                message.request_id,
#            ))
#        reply = self._send(message, HandoffFailedError)
#        self._log.debug(
#            'previous_size = %r' % (
#                reply.previous_size,
#            ))
#        return reply.previous_size
