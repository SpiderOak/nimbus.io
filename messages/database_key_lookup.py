# -*- coding: utf-8 -*-
"""
database_key_lookup.py

DatabaseKeyLookup message
"""
import struct

from tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
_header_format = "!32sQ"
_header_size = struct.calcsize(_header_format)

class DatabaseKeyLookup(object):
    """AMQP message to insert a key in the database"""

    routing_key = "database.key_insert"

    def __init__(
        self, 
        request_id, 
        avatar_id, 
        reply_exchange, 
        reply_routing_key, 
        key 
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_key = reply_routing_key
        self.key = key

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyLookup message"""
        pos = 0
        (request_id, avatar_id, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_key, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        return DatabaseKeyLookup(
            request_id, 
            avatar_id,
            reply_exchange, 
            reply_routing_key, 
            key 
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(_header_format, self.request_id, self.avatar_id)
        packed_reply_exchange = marshall_string(self.reply_exchange)
        packed_reply_routing_key = marshall_string(self.reply_routing_key)
        packed_key = marshall_string(self.key)
        return "".join(
            [
                header,
                packed_reply_exchange,
                packed_reply_routing_key,
                packed_key,
            ]
        )

