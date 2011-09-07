# -*- coding: utf-8 -*-
"""
space_accounting_client.py

Sends space accounting messages.
"""
import logging

from web_server.exceptions import SpaceUsageFailedError

class SpaceAccountingClient(object):
    """Sends space accounting messages."""

    def __init__(self, node_name, xreq_socket, push_socket):
        self._log = logging.getLogger("SpaceAccountingClient-%s" % (
            node_name, 
        ))
        self._xreq_socket = xreq_socket
        self._push_socket = push_socket

    def close(self):
        self._xreq_socket.close()
        self._push_socket.close()

    def added(self, collection_id, timestamp, bytes_added):
        message = {
            "message-type"  : "space-accounting-detail",
            "collection-id" : collection_id,
            "timestamp-repr": repr(timestamp),
            "event"         : "bytes_added",
            "value"         : bytes_added,
        }
        self._push_socket.send(message)

    def retrieved(self, collection_id, timestamp, bytes_retrieved):
        message = {
            "message-type"  : "space-accounting-detail",
            "collection-id" : collection_id,
            "timestamp-repr": repr(timestamp),
            "event"         : "bytes_retrieved",
            "value"         : bytes_retrieved,
        }
        self._push_socket.send(message)

    def removed(self, collection_id, timestamp, bytes_removed):
        message = {
            "message-type"  : "space-accounting-detail",
            "collection-id" : collection_id,
            "timestamp-repr": repr(timestamp),
            "event"         : "bytes_removed",
            "value"         : bytes_removed,
        }
        self._push_socket.send(message)

    def get_space_usage(
        self,
        collection_id
    ):
        request = {
            "message-type"  : "space-usage-request",
            "collection-id" : collection_id,
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(request)

        self._log.debug(
            '%(message-type)s: ' % request            
        )

        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            raise SpaceUsageFailedError(reply["error-message"])

        return {
            'bytes_added'       : reply["bytes-added"],
            'bytes_removed'     : reply["bytes-removed"],
            'bytes_retrieved'   : reply["bytes-retrieved"],
        }

