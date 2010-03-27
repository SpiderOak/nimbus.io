# -*- coding: utf-8 -*-
"""
archive_key_start.py

ArchiveKeyStart message
"""
from collections import namedtuple
import struct

from tools.marshalling import marshall_string, unmarshall_string

_header_tuple = namedtuple("Header", [ 
    "request_id", 
    "avatar_id", 
    "timestamp",
    "sequence",
    "segment_number",
    "segment_size",
])

# 32s - request-id 32 char hex uuid
# Q   - avatar_id 
# d   - timestamp
# I   - sequence
# B   - segment_number
# I   - segment size
_header_format = "32sQdIBI"
_header_size = struct.calcsize(_header_format)

class ArchiveKeyStart(object):
    """AMQP message to start archiving a key"""

    routing_key = "data_writer.archive_key_start"

    def __init__(
        self, 
        request_id,
        avatar_id, 
        reply_exchange,
        reply_routing_header,
        key, 
        timestamp, 
        sequence, 
        segment_number, 
        segment_size,
        data_content
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header
        self.key = key
        self.timestamp = timestamp
        self.sequence = sequence
        self.segment_number = segment_number
        self.segment_size = segment_size
        self.data_content = data_content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyStart message"""
        pos = 0
        header = _header_tuple._make(struct.unpack(
            _header_format, data[pos:pos+_header_size]
        ))
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        (key, pos) = unmarshall_string(data, pos)
        data_content = data[pos:]
        return ArchiveKeyStart(
            header.request_id, 
            header.avatar_id, 
            reply_exchange,
            reply_routing_header,
            key, 
            header.timestamp,
            header.sequence,
            header.segment_number, 
            header.segment_size, 
            data_content
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.avatar_id,
            self.timestamp,
            self.sequence,
            self.segment_number,
            self.segment_size
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
                self.data_content
            ]
        )


