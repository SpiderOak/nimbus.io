# -*- coding: utf-8 -*-
"""
purge_key_reply.py

PurgeKeyReply message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
_header_format = "!32sB"
_header_size = struct.calcsize(_header_format)

class PurgeKeyReply(object):
    """AMQP message reporting results of PurgeKey"""
   
    routing_tag = "purge_key_reply"
    
    successful = 0
    error_exception = 2
    error_no_such_key = 3
    error_invalid_duplicate = 4
    error_database_error = 5
    error_timeout_waiting_key_purge = 6

    def __init__(self, request_id, result, error_message=""):
        self.request_id = request_id
        self.result = result
        self.error_message = error_message

    @property
    def error(self):
        return self.result != PurgeKeyReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyInsert message"""
        pos = 0
        request_id, result, = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            return PurgeKeyReply(request_id, result)

        (error_message, pos) = unmarshall_string(data, pos)

        return PurgeKeyReply(request_id, result, error_message)

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result, 
        )
        packed_error_message = marshall_string(self.error_message)
         
        return "".join([header, packed_error_message, ])

