# -*- coding: utf-8 -*-
"""
space_usage_reply.py

SpaceUsageReply message
"""
import struct
from collections import namedtuple

from diyapi_tools.marshalling import marshall_string, unmarshall_string

_header_tuple = namedtuple("Header", [ 
    "request_id",
    "result",
    "bytes_added",
    "bytes_removed",
    "bytes_retrieved"
])

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success
# Q   - bytes added
# Q   - bytes removed
# Q   - bytes_retrieved
_header_format = "!32sBQQQ"
_header_size = struct.calcsize(_header_format)

class SpaceUsageReply(object):
    """AMQP message to reply with space usage information"""

    routing_tag = "space_usage_reply"

    successful = 0

    def __init__(
        self,
        request_id,
        result,
        bytes_added=0,
        bytes_removed=0,
        bytes_retrieved=0,
        error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.bytes_added = bytes_added
        self.bytes_removed = bytes_removed
        self.bytes_retrieved = bytes_retrieved
        self.error_message=error_message

    @property
    def error(self):
        return self.result != SpaceUsageReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a SpaceUsageReply message"""
        pos = 0
        header = _header_tuple._make(struct.unpack(
            _header_format, data[pos:pos+_header_size]
        ))
        pos += _header_size

        if header.result == 0:
            return SpaceUsageReply(
                header.request_id,
                header.result,
                header.bytes_added,
                header.bytes_removed,
                header.bytes_retrieved
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return SpaceUsageReply(
            request_id,
            result,
            error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.result,
            self.bytes_added,
            self.bytes_removed,
            self.bytes_retrieved
        )
        packed_error_message = marshall_string(self.error_message)
        return "".join([header, packed_error_message, ])

