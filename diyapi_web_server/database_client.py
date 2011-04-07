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

    def __init__(self, node_name, xreq_socket):
        self._log = logging.getLogger("DatabaseClient-%s" % (node_name, ))
        self._xreq_socket = xreq_socket

    def close(self):
        self._xreq_socket.close()

    def listmatch(
        self,
        request_id,
        avatar_id,
        prefix
    ):
        message = {
            "message-type"      : "listmatch",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "prefix"            : prefix,
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
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
        request_id,
        avatar_id,
        path,
        version_number=0,
    ):
        message = {
            "message-type"      : "stat-request",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "key"               : path, 
            "version-number"    : version_number,
        }
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
            'path = %(key)r' % message
            )

        delivery_channel = self._xreq_socket.queue_message_for_send(message)
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            raise StatFailedError(reply["error-message"])

        # we encode the md5 digest, because json doesn't like the raw string
        if reply["file_md5"] is not None:
            reply["file_md5"] = b64decode(reply["file_md5"])
        
        return reply

