# -*- coding: utf-8 -*-
"""
archive_key_entire.py

ArchiveKeyEntire message
"""
from collections import namedtuple
import struct

from tools.marshalling import marshall_string, unmarshall_string

_header_tuple = namedtuple("Header", [ 
    "request_id", 
    "avatar_id", 
    "timestamp",
    "segment_number",
    "adler32",
    "md5"
])

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# d   - timestamp
# B   - segment_number
# l   - adler32
# 16s - md5
_header_format = "32sQdBl16s"
_header_size = struct.calcsize(_header_format)

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
        reply_routing_header,
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
        self.reply_routing_header = reply_routing_header
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
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        content = data[pos:]
        return ArchiveKeyEntire(
            header.request_id, 
            header.avatar_id, 
            reply_exchange,
            reply_routing_header,
            key, 
            header.timestamp, 
            header.segment_number, 
            header.adler32, 
            header.md5, 
            content
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.avatar_id,
            self.timestamp,
            self.segment_number,
            self.adler32,
            self.md5
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
                self.content
            ]
        )

