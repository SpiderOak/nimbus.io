# -*- coding: utf-8 -*-
"""
data_reader.py

A class that represents a data reader in the system.
"""
import logging

from diyapi_web_server.exceptions import RetrieveFailedError

class DataReader(object):

    def __init__(self, node_name, xreq_socket):
        self._log = logging.getLogger("DataReader-%s" % (node_name, ))
        self._xreq_socket = xreq_socket

    def retrieve_key_start(
        self,
        request_id,
        avatar_id,
        key,
        version_number,
        segment_number
    ):
        message = {
            "message-type"      : "retrieve-key-start",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
            'key = %(key)r '
            'segment_number = %(segment-number)d' % message
            )
        reply, data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise RetrieveFailedError(reply["error-message"])
        return reply["segment-count"], data

    def retrieve_key_next(
        self,
        request_id,
        sequence_number
    ):
        message = {
            "message-type"      : "retrieve-key-next",
            "request-id"        : request_id,
            "sequence"          : sequence_number,
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
            'sequence_number = %(sequence)d' % message
            )
        reply, data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise RetrieveFailedError(reply["error-message"])
        return data

    def retrieve_key_final(
        self,
        request_id,
        sequence_number
    ):
        message = {
            "message-type"      : "retrieve-key-final",
            "request-id"        : request_id,
            "sequence"          : sequence_number,
        }
        delivery_channel = self._xreq_socket.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
            'request_id = %(request-id)s, '
            'sequence_number = %(sequence)d' % message
            )
        reply, data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise RetrieveFailedError(reply["error-message"])
        return data

