
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
    "segment_number",
    "timestamp",
])

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# B   - segment number
# d   - timestamp
_header_format = "32sQBd"
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
        key, 
        segment_number,
        timestamp 
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header
        self.key = key
        self.segment_number = segment_number
        self.timestamp = timestamp

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
            key, 
            header.segment_number,
            header.timestamp 
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.avatar_id,
            self.segment_number,
            self.timestamp
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

