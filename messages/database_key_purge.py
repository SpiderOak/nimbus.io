# -*- coding: utf-8 -*-
"""
database_key_purge.py

DatabaseKeyPurge message

This totaly removes the key/version-number/segment-number from the database
Intended for use by the handoff server
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id
# d   - timestamp
# I   - version_number
# B   - segment_number
_header_format = "!32sQdIB"
_header_size = struct.calcsize(_header_format)

class DatabaseKeyPurge(object):
    """AMQP message to insert a key in the database"""

    routing_key = "database_server.key_destroy"

    def __init__(
        self, 
        request_id, 
        avatar_id, 
        reply_exchange, 
        reply_routing_header, 
        timestamp,
        key,
        version_number,
        segment_number,
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
        """return a DatabaseKeyPurge message"""
        pos = 0
        (request_id, avatar_id, timestamp, version_number, segment_number, ) = \
            struct.unpack(_header_format, data[pos:pos+_header_size])
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        return DatabaseKeyPurge(
            request_id, 
            avatar_id,
            reply_exchange, 
            reply_routing_header, 
            timestamp,
            key,
            version_number,
            segment_number,
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

