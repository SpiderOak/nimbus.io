# -*- coding: utf-8 -*-
"""
archive_key_entire.py

ArchiveKeyEntire message
"""
from collections import namedtuple
import struct

_header_tuple = namedtuple("Header", [ 
    "request_id", 
    "avatar_id", 
    "reply_exchange_size", 
    "reply_routing_key_size", 
    "key_size",
    "timestamp",
    "segment_number",
    "adler32",
    "md5"
])

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# I   - reply-exchange size
# I   - reply-key size
# I   - key size
# d   - timestamp
# B   - segment_number
# l   - adler32
# 16s - md5
_header_format = "!32sQIIIdBl16s"
_header_size = struct.calcsize(_header_format)

_string_format = "%ds"

class ArchiveKeyEntire(object):
    """
    AMQP message to archive a key wiht the entire content contained in this
    message
    """
    routing_key = "data_writer.archive_key_entire"

    def __init__( 
        self, 
        request_id, 
        avatar_id, 
        reply_exchange,
        reply_routing_key,
        key, 
        timestamp, 
        segment_number, 
        adler32, 
        md5, 
        content
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_key = reply_routing_key
        self.key = key
        self.timestamp = timestamp
        self.segment_number = segment_number
        self.adler32 = adler32
        self.md5 = md5
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyEntire message"""
        pos = 0
        header = _header_tuple._make(struct.unpack(
            _header_format, data[pos:pos+_header_size]
        ))
        pos += _header_size
        (reply_exchange, ) = struct.unpack(
            _string_format % (header.reply_exchange_size, ),
            data[pos:pos+header.reply_exchange_size]
        )
        pos += header.reply_exchange_size
        (reply_routing_key, ) = struct.unpack(
            _string_format % (header.reply_routing_key_size, ), 
            data[pos:pos+header.reply_routing_key_size]
        )
        pos += header.reply_routing_key_size
        (key, ) = struct.unpack(
            _string_format % (header.key_size, ), 
            data[pos:pos+header.key_size]
        )
        pos += header.key_size
        content = data[pos:]
        return ArchiveKeyEntire(
            header.request_id, 
            header.avatar_id, 
            reply_exchange,
            reply_routing_key,
            key, 
            header.timestamp, 
            header.segment_number, 
            header.adler32, 
            header.md5, 
            content
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        reply_exchange_size = len(self.reply_exchange)
        reply_routing_key_size = len(self.reply_routing_key)
        key_size = len(self.key)
        header = struct.pack(
            _header_format,
            self.request_id,
            self.avatar_id,
            reply_exchange_size, 
            reply_routing_key_size, 
            key_size, 
            self.timestamp,
            self.segment_number,
            self.adler32,
            self.md5
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
                self.content
            ]
        )

