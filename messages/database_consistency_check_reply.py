# -*- coding: utf-8 -*-
"""
database_consistence_check_reply.py

DatabaseConsistencyCheckReply message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
# 16s - hash
_header_format = "!32sB16s"
_header_size = struct.calcsize(_header_format)

class DatabaseConsistencyCheckReply(object):
    """AMQP message returning a consistency hash for comparison"""

    routing_tag = "database_consistence_check_reply"
   
    successful = 0
    error_database_failure = 2

    def __init__(self, request_id, result, hash="", error_message=""):
        self.request_id = request_id
        self.result = result
        self.hash = hash
        self.error_message = error_message

    @property
    def error(self):
        return self.result != DatabaseConsistencyCheckReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseConsistencyCheck message"""
        pos = 0
        request_id, result, hash = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            return DatabaseConsistencyCheckReply(request_id, result, hash=hash)

        (error_message, pos) = unmarshall_string(data, pos)

        return DatabaseConsistencyCheckReply(
            request_id, result, error_message=error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result, 
            self.hash 
        )
        packed_error_message = marshall_string(self.error_message)

        return "".join([header, packed_error_message, ])

