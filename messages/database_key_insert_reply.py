# -*- coding: utf-8 -*-
"""
database_key_insert_reply.py

DatabaseKeyInsertReply message
"""
import struct

from tools.marshalling import marshall_string, unmarshall_string
from diyapi_database_server import database_content

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
# Q   - previous size
_header_format = "!32sBQ"
_header_size = struct.calcsize(_header_format)

class DatabaseKeyInsertReply(object):
    """AMQP message to insert a key in the database"""
   
    successful = 0
    error_invalid_duplicate = 1

    def __init__(
        self, request_id, result, previous_size=0, error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.previous_size = previous_size
        self.error_message = error_message

    @property
    def error(self):
        return self.result != DatabaseKeyInsertReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyInsert message"""
        pos = 0
        request_id, result, previous_size = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            return DatabaseKeyInsertReply(request_id, result, previous_size)

        (error_message, pos) = unmarshall_string(data, pos)

        return DatabaseKeyInsertReply(
            request_id, result, previous_size, error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result, 
            self.previous_size 
        )
        packed_error_message = marshall_string(self.error_message)

        return "".join([header, packed_error_message, ])

