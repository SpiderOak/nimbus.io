# -*- coding: utf-8 -*-
"""
database_consistency_check.py

DatabaseConsistencyCheck message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# d   - timestamp
_header_format = "!32sQd"
_header_size = struct.calcsize(_header_format)

class DatabaseConsistencyCheck(object):
    """AMQP message to insert a key in the database"""

    routing_key = "database_server.consistency_check"

    def __init__(
        self, 
        request_id, 
        avatar_id, 
        timestamp, 
        reply_exchange,
        reply_routing_header
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.timestamp = timestamp
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseConsistencyCheck message"""
        pos = 0
        (request_id, avatar_id, timestamp, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        return DatabaseConsistencyCheck(
            request_id, 
            avatar_id, 
            timestamp, 
            reply_exchange, 
            reply_routing_header
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, self.request_id, self.avatar_id, self.timestamp
        )
        packed_reply_exchange = marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(
            self.reply_routing_header
        )
        return "".join([
            header, packed_reply_exchange, packed_reply_routing_header
        ])

