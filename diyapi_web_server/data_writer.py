# -*- coding: utf-8 -*-
"""
data_writer.py

A class that represents a data writer in the system.
"""
from base64 import b64encode
import logging

import gevent

from diyapi_web_server.exceptions import (
    DataWriterDownError,
    ArchiveFailedError,
    DestroyFailedError,
)

class DataWriter(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DataWriter-%s" % (node_name, ))
        self._resilient_client = resilient_client

    @property
    def connected(self):
        return self._resilient_client.connected

    def archive_key_entire(
        self,
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
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
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
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "sequence"          : sequence_number,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "segment-size"      : len(segment)
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
            'key = %(key)r '
            'segment_number = %(segment-number)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_next(
        self,
        avatar_id,
        key,
        sequence_number,
        segment
    ):
        message = {
            "message-type"      : "archive-key-next",
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence_number,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: %(avatar-id)s $(key)s '
            'sequence = %(sequence)s' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_final(
        self,
        avatar_id,
        key,
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
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence_number,
            "total-size"        : file_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug('%(message-type)s: %(avatar-id)s %(key)s' % message)
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

        self._log.debug('previous_size = %(previous-size)r' % reply)
        return reply["previous-size"]

    def destroy_key(
        self,
        avatar_id,
        timestamp,
        key,
        segment_number,
        version_number
    ):
        message = {
            "message-type"      : "destroy-key",
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number,
        }
        delivery_channel = \
                self._resilient_client.queue_message_for_send(message)

        self._log.debug(
            '%(message-type)s: '
            'key = %(key)r '
            'segment_number = %(segment-number)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise DestroyFailedError(reply["error-message"])
        self._log.debug('total_size = %(total-size)r' % reply)
        return reply["total-size"]

