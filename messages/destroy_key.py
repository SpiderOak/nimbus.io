
# -*- coding: utf-8 -*-
"""
destroy_key.py

DestroyKey message
"""
from collections import namedtuple
import struct

from tools.marshalling import marshall_string, unmarshall_string

_header_tuple = namedtuple("Header", [ 
    "request_id", 
    "avatar_id", 
    "timestamp",
    "version_number",
    "segment_number",
])

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# d   - timestamp
# I   - version number
# B   - segment number
_header_format = "32sQdIB"
_header_size = struct.calcsize(_header_format)

class DestroyKey(object):
    """
    AMQP message to remove a key with its entire content
    """
    routing_key = "data_writer.destroy_key"

    def __init__( 
        self, 
        request_id, 
        avatar_id, 
        reply_exchange,
        reply_routing_header,
        timestamp,
        key, 
        segment_number,
        version_number
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header
        self.timestamp = timestamp
        self.key = key
        self.version_number = version_number
        self.segment_number = segment_number

    @classmethod
    def unmarshall(cls, data):
        """return a DestroyKey message"""
        pos = 0
        header = _header_tuple._make(struct.unpack(
            _header_format, data[pos:pos+_header_size]
        ))
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        return DestroyKey(
            header.request_id, 
            header.avatar_id, 
            reply_exchange,
            reply_routing_header,
            header.timestamp ,
            key, 
            header.version_number,
            header.segment_number,
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.avatar_id,
            self.timestamp,
            self.version_number,
            self.segment_number,
        )

        packed_reply_exchange =  marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(self.reply_routing_header)
        packed_key = marshall_string(self.key)
        return "".join(
            [
                header,
                packed_reply_exchange,
                packed_reply_routing_header,
                packed_key,
            ]
        )

