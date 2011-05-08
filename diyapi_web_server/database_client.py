# -*- coding: utf-8 -*-
"""
database_client.py

A class that connects to a specific database server.
"""
from base64 import b64decode
import logging

from diyapi_web_server.exceptions import (
    ListmatchFailedError,
    StatFailedError,
)

class DatabaseClient(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DatabaseClient-%s" % (node_name, ))
        self._resilient_client = resilient_client

    @property
    def connected(self):
        return self._resilient_client.connected

    def close(self):
        self._resilient_client.close()

    def listmatch(
        self,
        avatar_id,
        prefix
    ):
        message = {
            "message-type"      : "listmatch",
            "avatar-id"         : avatar_id,
            "prefix"            : prefix,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message
        )
        self._log.debug(
            '%(message-type)s: '
            'prefix = %(prefix)r' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            raise ListmatchFailedError(reply["error-message"])
        # TODO: need to handle incomplete reply
        if not reply["is-complete"]:
            self._log.error("incomplete reply to %s" % (message, ))
        return reply["key-list"]

    def stat(
        self,
        avatar_id,
        path,
        version_number=0,
    ):
        message = {
            "message-type"      : "stat-request",
            "avatar-id"         : avatar_id,
            "key"               : path, 
            "version-number"    : version_number,
        }
        self._log.debug(
            '%(message-type)s: '
            'path = %(key)r' % message
            )

        delivery_channel = self._resilient_client.queue_message_for_send(
            message
        )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            raise StatFailedError(reply["error-message"])

        # the caller compares these things for equality, so only give him
        # what he asked for
        return {
            "timestamp"     : reply["timestamp"],
            "total_size"    : reply["total_size"],
            "file_adler32"  : reply["file_adler32"],
            "file_md5"      : b64decode(reply["file_md5"]),
            "userid"        : reply["userid"],
            "groupid"       : reply["groupid"],
            "permissions"   : reply["permissions"],
        }

