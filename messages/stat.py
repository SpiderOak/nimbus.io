# -*- coding: utf-8 -*-
"""
stat.py

Stat message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id
# I   - version_number
_header_format = "!32sQI"
_header_size = struct.calcsize(_header_format)

class Stat(object):
    """AMQP message to stat"""

    routing_key = "database_server.stat"

    def __init__(
        self,
        request_id,
        avatar_id,
        key,
        version_number,
        reply_exchange,
        reply_routing_header
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = version_number
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header

    @classmethod
    def unmarshall(cls, data):
        """return a Stat message"""
        pos = 0
        (request_id, avatar_id, version_number) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (key, pos) = unmarshall_string(data, pos)
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        return Stat(
            request_id,
            avatar_id,
            key,
            version_number,
            reply_exchange,
            reply_routing_header
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.avatar_id,
            self.version_number
        )
        key = marshall_string(self.key)
        packed_reply_exchange = marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(
            self.reply_routing_header)
        return "".join(
            [
                header,
                key,
                packed_reply_exchange,
                packed_reply_routing_header
            ]
        )

