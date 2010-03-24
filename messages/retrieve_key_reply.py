# -*- coding: utf-8 -*-
"""
retrieve_key_reply.py

RetrieveKeyReply message
"""
import struct

from tools.marshalling import marshall_string, unmarshall_string
from diyapi_database_server import database_content

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
_header_format = "!32sB"
_header_size = struct.calcsize(_header_format)

class RetrieveKeyReply(object):
    """AMQP message to request retrieval of a key"""
   
    successful = 0
    error_key_not_found = 1
    error_exception = 2
    error_database = 3
    error_invalid_duplicate = 4

    def __init__(
        self, 
        request_id, 
        result, 
        database_content="",
        data_content="",
        error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.database_content = database_content
        self.data_content = data_content
        self.error_message = error_message

    @property
    def error(self):
        return self.result != ArchiveKeyEntireReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a RetrieveKeyReply message"""
        pos = 0
        request_id, result = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            (db_content, pos) = database_content.unmarshall(data, pos)
            return RetrieveKeyReply(                
                request_id, 
                result, 
                database_content=db_content, 
                data_content=data[pos:]
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return ArchiveKeyEntireReply(
            request_id, result, error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        error_message_size = len(self.error_message)
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result, 
        )

        if self.result == 0:
            packed_database_content = database_content.marshall(
                self.database_content
            )
            return "".join(
                [header, packed_database_content, self.data_content]
            )

        packed_error_message = marshall_string(self.error_message)
         
        return "".join([header, packed_error_message, ])

