# -*- coding: utf-8 -*-
"""
retrieve_key_next_reply.py

RetrieveKeyNextReply message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string
from diyapi_database_server import database_content

# 32s - request-id 32 char hex uuid
# I   - sequence
# B   - result: 0 = success 
_header_format = "!32sIB"
_header_size = struct.calcsize(_header_format)

class RetrieveKeyNextReply(object):
    """AMQP message to request retrieval of a key"""
   
    routing_tag = "retrieve_key_next_reply"

    successful = 0
    error_key_not_found = 1
    error_exception = 2
    error_database = 3
    error_invalid_duplicate = 4
    error_out_of_sequence = 5

    def __init__(
        self, 
        request_id, 
        sequence,
        result,
        data_content="",
        error_message=""
    ):
        self.request_id = request_id
        self.sequence = sequence
        self.result = result
        self.data_content = data_content
        self.error_message = error_message

    @property
    def error(self):
        return self.result != RetrieveKeyNextReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a RetrieveKeyNextReply message"""
        pos = 0
        request_id, sequence, result = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            return RetrieveKeyNextReply(                
                request_id, 
                sequence,
                result, 
                data_content=data[pos:]
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return RetrieveKeyNextReply(
            request_id, result, error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id,
            self.sequence,
            self.result, 
        )

        if self.result == 0:
            return "".join([header, self.data_content])

        packed_error_message = marshall_string(self.error_message)
         
        return "".join([header, packed_error_message, ])

