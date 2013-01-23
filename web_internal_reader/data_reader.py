# -*- coding: utf-8 -*-
"""
data_reader.py

A class that represents a data reader in the system.
"""
from base64 import b64decode
import hashlib
import logging

from tools.greenlet_resilient_client import ResilientClientError

class DataReader(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DataReader-%s" % (node_name, ))
        self._node_name = node_name
        self._resilient_client = resilient_client

    @property
    def connected(self):
        return self._resilient_client.connected

    @property
    def node_name(self):
        return self._node_name

    def retrieve_key_start(self, 
                           retrieve_id,
                           sequence,
                           collection_id,
                           key,
                           segment_unified_id, 
                           segment_conjoined_part, 
                           segment_num,
                           block_offset,
                           block_count,
                           user_request_id):
        message = {
            "message-type"              : "retrieve-key-start",
            "user-request-id"           : user_request_id,            
            "retrieve-id"               : retrieve_id,
            "retrieve-sequence"         : sequence,
            "collection-id"             : collection_id,
            "key"                       : key,
            "segment-unified-id"        : segment_unified_id,
            "segment-conjoined-part"    : segment_conjoined_part,
            "segment-num"               : segment_num,
            "handoff-node-id"           : None,
            "block-offset"              : block_offset,
            "block-count"               : block_count,
        }
        try:
            delivery_channel = \
                    self._resilient_client.queue_message_for_send(message)
        except ResilientClientError:
            self._log.exception("request {0}".format(user_request_id))
            return None

        self._log.debug("request: {user-request-id} " \
                        "{message-type}: {segment-unified-id} {segment-num}".format(
                        **message))

        reply, data = delivery_channel.get()

        if reply["result"] != "success":
            self._log.error("request {0} failed: {1}".format(user_request_id,
                                                             reply))
            return None

        # we expect a list of blocks, but if the data is smaller than 
        # block size, we get back a string
        if type(data) != list:
            data = [data, ]

        # Ticket #1307 danger of zfec bit rot
        # we must make sure we are handing zfec valid segments to reassemble
        segment_size = 0
        segment_md5 = hashlib.md5()
        for block in data:
            segment_size += len(block)
            segment_md5.update(block)

        if segment_size != reply["segment-size"]:
            self._log.error("request {0} failed: " \
                            "data size is {1} expecting {2} {3}".format(
                user_request_id, segment_size, reply["segment-size"], reply))
            return None

        if segment_md5.digest() != b64decode(reply["segment-md5-digest"]):
            self._log.error("request {0} md5 digest mismatch {1}".format(
                            user_request_id, reply))
            return None

        return data, reply["zfec-padding-size"], reply["completed"]

    def retrieve_key_next(self, 
                          retrieve_id,
                          sequence,
                          collection_id,
                          key,
                          segment_unified_id, 
                          segment_conjoined_part, 
                          segment_num,
                          block_offset,
                          block_count,
                          user_request_id):
        message = {
            "message-type"              : "retrieve-key-next",
            "user-request-id"           : user_request_id,
            "retrieve-id"               : retrieve_id,
            "retrieve-sequence"         : sequence,
            "collection-id"             : collection_id,
            "key"                       : key,
            "segment-unified-id"        : segment_unified_id,
            "segment-conjoined-part"    : segment_conjoined_part,
            "segment-num"               : segment_num,
            "handoff-node-id"           : None,
            "block-offset"              : block_offset,
            "block-count"               : block_count,
        }
        try:
            delivery_channel = \
                    self._resilient_client.queue_message_for_send(message)
        except ResilientClientError:
            self._log.exception("request {0}".format(user_request_id))
            return None

        self._log.debug("request: {user-request-id} " \
                        "{message-type}: {segment-unified-id} {segment-num}".format(
                        **message))
        reply, data = delivery_channel.get()

        if reply["result"] != "success":
            self._log.error("request {0} failed: {1}".format(user_request_id,
                                                             reply))
            return None

        # we expect a list of blocks, but if the data is smaller than 
        # block size, we get back a string
        if type(data) != list:
            data = [data, ]

        # Ticket #1307 danger of zfec bit rot
        # we must make sure we are handing zfec valid segments to reassemble
        segment_size = 0
        segment_md5 = hashlib.md5()
        for block in data:
            segment_size += len(block)
            segment_md5.update(block)

        if segment_size != reply["segment-size"]:
            self._log.error("request {0} failed: " \
                            "data size is {1} expecting {2} {3}".format(
                user_request_id, segment_size, reply["segment-size"], reply))
            return None

        if segment_md5.digest() != b64decode(reply["segment-md5-digest"]):
            self._log.error("request {0} md5 digest mismatch {1}".format(
                            user_request_id, reply))
            return None

        return data, reply["zfec-padding-size"], reply["completed"]
