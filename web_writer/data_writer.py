# -*- coding: utf-8 -*-
"""
data_writer.py

A class that represents a data writer in the system.
"""
from base64 import b64encode
import hashlib
import logging
import zlib

from tools.data_definitions import create_priority

def _segment_properties(segment):
    segment_size = 0
    segment_adler32 = 0
    segment_md5 = hashlib.md5()
    for data_block in segment:
        segment_size += len(data_block)
        segment_adler32 = zlib.adler32(data_block)
        segment_md5.update(data_block)

    return segment_size, segment_adler32, segment_md5

class DataWriter(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DataWriter-%s" % (node_name, ))
        self._node_name = node_name
        self._resilient_client = resilient_client
        self._archive_priority = None

    @property
    def connected(self):
        return self._resilient_client.connected

    @property
    def node_name(self):
        return self._node_name

    def archive_key_entire(
        self,
        collection_id,
        key,
        unified_id,
        timestamp,
        conjoined_part,
        meta_dict,
        segment_num,
        zfec_padding_size,
        file_size,
        file_adler32,
        file_md5,
        segment,
        source_node_name,
        user_request_id
    ):
        segment_size, segment_adler32, segment_md5 = \
                _segment_properties(segment)

        message = {
            "message-type"              : "archive-key-entire",
            "priority"                  : create_priority(),
            "user-request-id"           : user_request_id,
            "collection-id"             : collection_id,
            "key"                       : key, 
            "unified-id"                : unified_id,
            "timestamp-repr"            : repr(timestamp),
            "conjoined-part"            : conjoined_part,
            "segment-num"               : segment_num,
            "segment-size"              : segment_size,
            "zfec-padding-size"         : zfec_padding_size,
            "segment-md5-digest"        : b64encode(segment_md5.digest()),
            "segment-adler32"           : segment_adler32,
            "file-size"                 : file_size,
            "file-adler32"              : file_adler32,
            "file-hash"                 : b64encode(file_md5),
            "source-node-name"          : source_node_name,
            "handoff-node-name"         : None,
        }
        message.update(meta_dict)
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug("request {user-request-id}: {message-type}: " \
                        "key = {key} " \
                        "timestamp = {timestamp-repr} " \
                        "segment_num = {segment-num}".format(**message))
        reply, _data = delivery_channel.get()
        return reply

    def archive_key_start(
        self,
        collection_id,
        key,
        unified_id,
        timestamp,
        conjoined_part,
        segment_num,
        zfec_padding_size,
        sequence_num,
        segment,
        source_node_name,
        user_request_id
    ):
        segment_size, segment_adler32, segment_md5 = \
                _segment_properties(segment)

        self._archive_priority = create_priority()

        message = {
            "message-type"          : "archive-key-start",
            "priority"              : self._archive_priority,
            "user-request-id"       : user_request_id,
            "collection-id"         : collection_id,
            "key"                   : key, 
            "unified-id"            : unified_id,
            "timestamp-repr"        : repr(timestamp),
            "conjoined-part"        : conjoined_part,
            "segment-num"           : segment_num,
            "segment-size"          : segment_size,
            "zfec-padding-size"     : zfec_padding_size,
            "segment-md5-digest"    : b64encode(segment_md5.digest()),
            "segment-adler32"       : segment_adler32,
            "sequence-num"          : sequence_num,
            "source-node-name"      : source_node_name,
            "handoff-node-name"     : None,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug("request {user-request-id}: {message-type}: " \
                        "key = {key} " \
                        "timestamp = {timestamp-repr} " \
                        "segment_num = {segment-num}".format(**message))
        reply, _data = delivery_channel.get()
        return reply

    def archive_key_next(
        self,
        collection_id,
        key,
        unified_id,
        timestamp,
        conjoined_part,
        segment_num,
        zfec_padding_size,
        sequence_num,
        segment,
        source_node_name,
        user_request_id,
    ):
        segment_size, segment_adler32, segment_md5 = \
                _segment_properties(segment)

        message = {
            "message-type"          : "archive-key-next",
            "priority"              : self._archive_priority,
            "user-request-id"       : user_request_id,
            "collection-id"         : collection_id,
            "key"                   : key,
            "unified-id"            : unified_id,
            "timestamp-repr"        : repr(timestamp),
            "conjoined-part"        : conjoined_part,
            "segment-num"           : segment_num,
            "segment-size"          : segment_size,
            "zfec-padding-size"     : zfec_padding_size,
            "segment-md5-digest"    : b64encode(segment_md5.digest()),
            "segment-adler32"       : segment_adler32,
            "sequence-num"          : sequence_num,
            "source-node-name"      : source_node_name,
            "handoff-node-name"     : None,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug("request {user-request-id}: {message-type}: " \
                        "key = {key} " \
                        "timestamp = {timestamp-repr} " \
                        "segment_num = {segment-num}".format(**message))
        reply, _data = delivery_channel.get()
        return reply

    def archive_key_final(
        self,
        collection_id,
        key,
        unified_id,
        timestamp,
        conjoined_part,
        meta_dict,
        segment_num,
        zfec_padding_size,
        sequence_num,
        file_size,
        file_adler32,
        file_md5,
        segment,
        source_node_name,
        user_request_id
    ):
        segment_size, segment_adler32, segment_md5 = \
                _segment_properties(segment)

        message = {
            "message-type"              : "archive-key-final",
            "priority"                  : self._archive_priority,
            "user-request-id"           : user_request_id,
            "collection-id"             : collection_id,
            "key"                       : key,
            "unified-id"                : unified_id,
            "timestamp-repr"            : repr(timestamp),
            "conjoined-part"            : conjoined_part,
            "segment-num"               : segment_num,
            "segment-size"              : segment_size,
            "zfec-padding-size"         : zfec_padding_size,
            "segment-md5-digest"        : b64encode(segment_md5.digest()),
            "segment-adler32"           : segment_adler32,
            "sequence-num"              : sequence_num,
            "file-size"                 : file_size,
            "file-adler32"              : file_adler32,
            "file-hash"                 : b64encode(file_md5),
            "source-node-name"          : source_node_name,
            "handoff-node-name"         : None,
        }

        self._archive_priority = None

        message.update(meta_dict)
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug("request {user-request-id}: {message-type}: " \
                        "key = {key} " \
                        "timestamp = {timestamp-repr} " \
                        "segment_num = {segment-num}".format(**message))
        reply, _data = delivery_channel.get()
        return reply

    def destroy_key(
        self,
        collection_id,
        key,
        unified_id_to_delete,
        unified_id,
        timestamp,
        segment_num,
        source_node_name,
        user_request_id
    ):
        message = {
            "message-type"              : "destroy-key",
            "priority"                  : create_priority(),
            "user-request-id"           : user_request_id,
            "collection-id"             : collection_id,
            "key"                       : key,
            "unified-id-to-delete"      : unified_id_to_delete,
            "unified-id"                : unified_id,
            "timestamp-repr"            : repr(timestamp),
            "segment-num"               : segment_num,
            "source-node-name"          : source_node_name,
            "handoff-node-name"         : None,
        }
        delivery_channel = \
                self._resilient_client.queue_message_for_send(message)

        self._log.debug("request {user-request-id}: {message-type}: " \
                        "key = {key} " \
                        "timestamp = {timestamp-repr} " \
                        "segment_num = {segment-num}".format(**message))
        reply, _data = delivery_channel.get()
        return reply
