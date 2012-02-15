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

from web_server.exceptions import (
    ArchiveFailedError,
    DestroyFailedError,
)

class DataWriter(object):

    def __init__(self, node_name, resilient_client):
        self._log = logging.getLogger("DataWriter-%s" % (node_name, ))
        self._resilient_client = resilient_client
        self._archive_priority = None

    @property
    def connected(self):
        return self._resilient_client.connected

    def archive_key_entire(
        self,
        collection_id,
        key,
        unified_id,
        timestamp,
        meta_dict,
        conjoined_unified_id,
        conjoined_part,
        segment_num,
        zfec_padding_size,
        file_size,
        file_adler32,
        file_md5,
        segment,
        source_node_name,
    ):
        segment_md5 = hashlib.md5()
        segment_md5.update(segment)

        message = {
            "message-type"              : "archive-key-entire",
            "priority"                  : create_priority(),
            "collection-id"             : collection_id,
            "key"                       : key, 
            "unified-id"                : unified_id,
            "timestamp-repr"            : repr(timestamp),
            "conjoined-unified-id"      : conjoined_unified_id,
            "conjoined-part"            : conjoined_part,
            "segment-num"               : segment_num,
            "segment-size"              : len(segment),
            "zfec-padding-size"         : zfec_padding_size,
            "segment-md5-digest"        : b64encode(segment_md5.digest()),
            "segment-adler32"           : zlib.adler32(segment),
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
        collection_id,
        key,
        unified_id,
        timestamp,
        segment_num,
        zfec_padding_size,
        sequence_num,
        segment,
        source_node_name
    ):
        segment_md5 = hashlib.md5()
        segment_md5.update(segment)

        self._archive_priority = create_priority()

        message = {
            "message-type"          : "archive-key-start",
            "priority"              : self._archive_priority,
            "collection-id"         : collection_id,
            "key"                   : key, 
            "unified-id"            : unified_id,
            "timestamp-repr"        : repr(timestamp),
            "segment-num"           : segment_num,
            "segment-size"          : len(segment),
            "zfec-padding-size"     : zfec_padding_size,
            "segment-md5-digest"    : b64encode(segment_md5.digest()),
            "segment-adler32"       : zlib.adler32(segment),
            "sequence-num"          : sequence_num,
            "source-node-name"      : source_node_name,
            "handoff-node-name"     : None,
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
        collection_id,
        key,
        unified_id,
        timestamp,
        segment_num,
        zfec_padding_size,
        sequence_num,
        segment,
        source_node_name
    ):
        segment_md5 = hashlib.md5()
        segment_md5.update(segment)

        message = {
            "message-type"          : "archive-key-next",
            "priority"              : self._archive_priority,
            "collection-id"         : collection_id,
            "key"                   : key,
            "unified_id"            : unified_id,
            "timestamp-repr"        : repr(timestamp),
            "segment-num"           : segment_num,
            "segment-size"          : len(segment),
            "zfec-padding-size"     : zfec_padding_size,
            "segment-md5-digest"    : b64encode(segment_md5.digest()),
            "segment-adler32"       : zlib.adler32(segment),
            "sequence-num"          : sequence_num,
            "source-node-name"      : source_node_name,
            "handoff-node-name"     : None,
        }
        delivery_channel = self._resilient_client.queue_message_for_send(
            message, data=segment
        )
        self._log.debug(
            '%(message-type)s: %(collection-id)s $(key)s '
            'sequence = %(sequence-num)s' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def archive_key_final(
        self,
        collection_id,
        key,
        unified_id,
        timestamp,
        meta_dict,
        conjoined_unified_id,
        conjoined_part,
        segment_num,
        zfec_padding_size,
        sequence_num,
        file_size,
        file_adler32,
        file_md5,
        segment,
        source_node_name
    ):
        segment_md5 = hashlib.md5()
        segment_md5.update(segment)

        message = {
            "message-type"              : "archive-key-final",
            "priority"                  : self._archive_priority,
            "collection-id"             : collection_id,
            "key"                       : key,
            "unified-id"                : unified_id,
            "timestamp-repr"            : repr(timestamp),
            "conjoined-unified-id"      : conjoined_unified_id,
            "conjoined-part"            : conjoined_part,
            "segment-num"               : segment_num,
            "segment-size"              : len(segment),
            "zfec-padding-size"         : zfec_padding_size,
            "segment-md5-digest"        : b64encode(segment_md5.digest()),
            "segment-adler32"           : zlib.adler32(segment),
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
        self._log.debug(
            '%(message-type)s: %(collection-id)s %(key)s' % message
        )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise ArchiveFailedError(reply["error-message"])

    def destroy_key(
        self,
        collection_id,
        key,
        unified_id_to_delete,
        unified_id,
        timestamp,
        segment_num,
        source_node_name
    ):
        message = {
            "message-type"              : "destroy-key",
            "priority"                  : create_priority(),
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

        self._log.debug(
            '%(message-type)s: '
            'key = %(key)r '
            'segment_num = %(segment-num)d' % message
            )
        reply, _data = delivery_channel.get()
        if reply["result"] != "success":
            self._log.error("failed: %s" % (reply, ))
            raise DestroyFailedError(reply["error-message"])

