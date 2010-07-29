# -*- coding: utf-8 -*-
"""
stat_reply.py

StatReply message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success
_header_format = "!32sB"
_header_size = struct.calcsize(_header_format)

class StatReply(object):
    """AMQP message to reply with space usage information"""

    routing_tag = "stat_reply"

    successful = 0
    error_key_not_found = 1

    def __init__(
        self,
        request_id,
        result,
        error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.error_message=error_message

    @property
    def error(self):
        return self.result != StatReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a StatReply message"""
        pos = 0
        (request_id, result, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            return StatReply(
                request_id,
                result
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return StatReply(
            request_id,
            result,
            error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.result
        )
        packed_error_message = marshall_string(self.error_message)
        return "".join([header, packed_error_message, ])
