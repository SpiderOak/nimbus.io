# -*- coding: utf-8 -*-
"""
data_writer.py

A class that represents a data writer in the system.
"""
from base64 import b64encode
import logging

import gevent

from diyapi_web_server.exceptions import (
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
        key,
        timestamp,
        segment_num,
        file_size,
        file_adler32,
        file_md5,
        file_user_id,
        file_group_id,
        file_permissions,
        segment,
    ):
        message = {
            "message-type"      : "archive-key-entire",
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5),
            "file-user-id"      : file_user_id,
            "file-group-id"     : file_group_id,
            "file-permissions"  : file_permissions,
            "handoff-node-name" : None,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
            'key = %(key)r '
            'timestamp = %(timestamp-repr)r '
            'segment_num = %(segment-num)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_start(
        self,
        avatar_id,
        key,
        timestamp,
        segment_num,
        sequence_num,
        segment
    ):
        message = {
            "message-type"      : "archive-key-start",
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "sequence-num"      : sequence_num,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: '
            'key = %(key)r '
            'segment_num = %(segment-num)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_next(
        self,
        avatar_id,
        key,
        timestamp,
        segment_num,
        sequence_num,
        segment
    ):
        message = {
            "message-type"      : "archive-key-next",
            "avatar-id"         : avatar_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "sequence-num"      : sequence_num,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: %(avatar-id)s $(key)s '
            'sequence = %(sequence-num)s' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_final(
        self,
        avatar_id,
        key,
        timestamp,
        segment_num,
        sequence_num,
        file_size,
        file_adler32,
        file_md5,
        file_user_id,
        file_group_id,
        file_permissions,
        segment,
    ):
        message = {
            "message-type"      : "archive-key-final",
            "avatar-id"         : avatar_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "sequence-num"      : sequence_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5),
            "file-user-id"      : file_user_id,
            "file-group-id"     : file_group_id,
            "file-permissions"  : file_permissions,
            "handoff-node-name" : None,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug('%(message-type)s: %(avatar-id)s %(key)s' % message)
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def destroy_key(
        self,
        avatar_id,
        key,
        timestamp,
        segment_num
    ):
        message = {
            "message-type"      : "destroy-key",
            "avatar-id"         : avatar_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
        }
        delivery_channel = \
                self._resilient_client.queue_message_for_send(message)

        self._log.debug(
            '%(message-type)s: '
            'key = %(key)r '
            'segment_num = %(segment-num)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise DestroyFailedError(reply["error-message"])

