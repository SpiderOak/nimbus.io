# -*- coding: utf-8 -*-
"""
data_reader.py

A class that represents a data reader in the system.
"""
import logging

from diyapi_web_server.exceptions import RetrieveFailedError

class DataReader(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DataReader-%s" % (node_name, ))
        self._resilient_client = resilient_client

    def retrieve_key_start(
        self,
        avatar_id,
        key,
        version_number,
        segment_number
    ):
        message = {
            "message-type"      : "retrieve-key-start",
            "avatar-id"         : avatar_id,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number
        }
        delivery_channel = self._resilient_client.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
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
        sequence_number
    ):
        message = {
            "message-type"      : "retrieve-key-next",
            "sequence"          : sequence_number,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
            'sequence_number = %(sequence)d' % message
            )
        reply, data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise RetrieveFailedError(reply["error-message"])
        return data

    def retrieve_key_final(
        self,
        sequence_number
    ):
        message = {
            "message-type"      : "retrieve-key-final",
            "sequence"          : sequence_number,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: '
            'sequence_number = %(sequence)d' % message
            )
        reply, data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise RetrieveFailedError(reply["error-message"])
        return data

