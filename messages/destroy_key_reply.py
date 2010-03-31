# -*- coding: utf-8 -*-
"""
destroy_key_reply.py

DestroyKeyReply message
"""
import struct

from tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
# Q   - previous size
_header_format = "!32sBQ"
_header_size = struct.calcsize(_header_format)

class DestroyKeyReply(object):
    """AMQP message reporting results of DestroyKey"""
   
    routing_tag = "destroy_key_reply"
    
    successful = 0
    error_exception = 2
    error_invalid_duplicate = 3
    error_too_old = 4
    error_database_error = 5
    error_timeout_waiting_key_destroy = 6

    def __init__(
        self, request_id, result, total_size=0, error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.total_size = total_size
        self.error_message = error_message

    @property
    def error(self):
        return self.result != DestroyKeyReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyInsert message"""
        pos = 0
        request_id, result, total_size = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            return DestroyKeyReply(request_id, result, total_size)

        (error_message, pos) = unmarshall_string(data, pos)

        return DestroyKeyReply(
            request_id, result, total_size, error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result, 
            self.total_size 
        )
        packed_error_message = marshall_string(self.error_message)
         
        return "".join([header, packed_error_message, ])


