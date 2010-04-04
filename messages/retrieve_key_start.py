# -*- coding: utf-8 -*-
"""
retrieve_key_start.py

RetrieveKeyStart message
"""
import struct

from tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# B   - segment number
_header_format = "32sQB"
_header_size = struct.calcsize(_header_format)

class RetrieveKeyStart(object):
    """
    AMQP message to start retrieving a key 
    """
    routing_key = "data_reader.retrieve_key_start"

    def __init__( 
        self, 
        request_id, 
        avatar_id, 
        reply_exchange,
        reply_routing_header,
        key,
        segment_number
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header
        self.key = key
        self.segment_number = segment_number

    @classmethod
    def unmarshall(cls, data):
        """return a RetrieveKeyStart message"""
        pos = 0
        request_id, avatar_id, segment_number = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        return RetrieveKeyStart(
            request_id, 
            avatar_id, 
            reply_exchange,
            reply_routing_header,
            key,
            segment_number
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.avatar_id,
            self.segment_number
        )

        packed_reply_exchange =  marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(self.reply_routing_header)
        packed_key = marshall_string(self.key)
        return "".join(
            [
                header,
                packed_reply_exchange,
                packed_reply_routing_header,
                packed_key
            ]
        )

