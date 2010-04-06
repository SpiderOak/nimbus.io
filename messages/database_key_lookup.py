# -*- coding: utf-8 -*-
"""
database_key_lookup.py

DatabaseKeyLookup message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# I   - version_number
# B   - segment_number
_header_format = "!32sQIB"
_header_size = struct.calcsize(_header_format)

class DatabaseKeyLookup(object):
    """AMQP message to lookup a specfic entry for a key and segment number"""

    routing_key = "database_server.key_lookup"

    def __init__(
        self, 
        request_id, 
        avatar_id, 
        reply_exchange, 
        reply_routing_header, 
        key,
        version_number,
        segment_number
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header
        self.key = key
        self.version_number = version_number
        self.segment_number = segment_number

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyLookup message"""
        pos = 0
        (request_id, avatar_id, version_number, segment_number) = \
            struct.unpack(_header_format, data[pos:pos+_header_size])
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        return DatabaseKeyLookup(
            request_id, 
            avatar_id,
            reply_exchange, 
            reply_routing_header, 
            key,
            version_number,
            segment_number
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.avatar_id, 
            self.version_number & 0xFFFFFFFF,
            self.segment_number & 0xFF
        )
        packed_reply_exchange = marshall_string(self.reply_exchange)
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

