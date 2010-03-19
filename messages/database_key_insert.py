# -*- coding: utf-8 -*-
"""
database_key_insert.py

DatabaseKeyInsert message
"""
import struct

from diyapi_database_server import database_content

# 32s - request-id 32 char hex uuid
# I   - reply-exchange size
# I   - reply-key size
# I   - key size
_header_format = "!32sIII"
_header_size = struct.calcsize(_header_format)
_string_format = "%ds"

class DatabaseKeyInsert(object):
    """AMQP message to insert a key in the database"""

    routing_key = "database.key_insert"

    def __init__(
        self, request_id, reply_exchange, reply_routing_key, key, content
    ):
        self.request_id = request_id
        self.reply_exchange = reply_exchange
        self.reply_routing_key = reply_routing_key
        self.key = key
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyInsert message"""
        pos = 0
        request_id, reply_exchange_size, reply_routing_key_size, key_size = \
            struct.unpack(
                _header_format, data[pos:pos+_header_size]
            )
        pos += _header_size
        (reply_exchange, ) = struct.unpack(
            _string_format % (reply_exchange_size, ),
            data[pos:pos+reply_exchange_size]
        )
        pos += reply_exchange_size
        (reply_routing_key, ) = struct.unpack(
            _string_format % (reply_routing_key_size, ), 
            data[pos:pos+reply_routing_key_size]
        )
        pos += reply_routing_key_size
        (key, ) = struct.unpack(
            _string_format % (key_size, ), data[pos:pos+key_size]
        )
        pos += key_size
        content = database_content.unmarshall(data[pos:])
        return DatabaseKeyInsert(
            request_id, reply_exchange, reply_routing_key, key, content
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        reply_exchange_size = len(self.reply_exchange)
        reply_routing_key_size = len(self.reply_routing_key)
        key_size = len(self.key)
        header = struct.pack(
            _header_format, 
            self.request_id, 
            reply_exchange_size, 
            reply_routing_key_size, 
            key_size
        )
        packed_reply_exchange = struct.pack(
            _string_format % (reply_exchange_size, ), self.reply_exchange
        )
        packed_reply_routing_key = struct.pack(
            _string_format % (reply_routing_key_size, ), 
            self.reply_routing_key
        )
        packed_key = struct.pack(_string_format % (key_size, ), self.key)
        return "".join(
            [
                header,
                packed_reply_exchange,
                packed_reply_routing_key,
                packed_key,
                database_content.marshall(self.content)
            ]
        )

