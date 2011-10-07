# -*- coding: utf-8 -*-
"""
data_reader.py

A class that represents a data reader in the system.
"""
from base64 import b64decode
import hashlib
import logging

class DataReader(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DataReader-%s" % (node_name, ))
        self._node_name = node_name
        self._resilient_client = resilient_client

    @property
    def connected(self):
        return self._resilient_client

    @property
    def node_name(self):
        return self._node_name

    def retrieve_key_start(
        self,
        collection_id,
        key,
        timestamp,
        segment_num
    ):
        message = {
            "message-type"      : "retrieve-key-start",
            "collection-id"     : collection_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
        }
        delivery_channel = \
                self._resilient_client.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: %(collection-id)s '
            'key = %(key)r '
            'segment_num = %(segment-num)d' % message
            )

        reply, data = delivery_channel.get()

        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            return None

        # Ticket #1307 danger of zfec bit rot
        # we must make sure we are handing zfec valid segments to reassemble

        if len(data) != reply["segment-size"]:
            self._log.error("failed: data size is %s expecting %s %s" % (
                reply["segment-size"], len(data), reply
            ))
            return None

        segment_md5 = hashlib.md5(data)
        if segment_md5.digest() != b64decode(reply["segment-md5-digest"]):
            self._log.error("md5 digest mismatch %s" % (reply, ))
            return None

        return data, reply["completed"]

    def retrieve_key_next(
        self,
        collection_id,
        key,
        timestamp,
        segment_num
    ):
        message = {
            "message-type"      : "retrieve-key-next",
            "collection-id"     : collection_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
        }
        delivery_channel = \
                self._resilient_client.queue_message_for_send(message)
        self._log.debug(
            '%(message-type)s: %(collection-id)s %(key)s' % message
        )
        reply, data = delivery_channel.get()

        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            return None

        # Ticket #1307 danger of zfec bit rot
        # we must make sure we are handing zfec valid segments to reassemble

        if len(data) != reply["segment-size"]:
            self._log.error("failed: data size is %s expecting %s %s" % (
                reply["segment-size"], len(data), reply
            ))
            return None

        segment_md5 = hashlib.md5(data)
        if segment_md5.digest() != b64decode(reply["segment-md5-digest"]):
            self._log.error("md5 digest mismatch %s" % (reply, ))
            return None

        return data, reply["completed"]

