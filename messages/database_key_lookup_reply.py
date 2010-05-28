# -*- coding: utf-8 -*-
"""
database_key_lookip_reply.py

DatabaseKeyLookupReply message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string
from diyapi_database_server import database_content

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
_header_format = "!32sB"
_header_size = struct.calcsize(_header_format)

class DatabaseKeyLookupReply(object):
    """AMQP message to insert a key in the database"""

    routing_tag = "database_key_lookup_reply"
   
    successful = 0
    error_unknown_key = 1
    error_database_failure = 2

    def __init__(
        self, request_id, result, database_content="", error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.database_content = database_content
        self.error_message = error_message

    @property
    def error(self):
        return self.result != DatabaseKeyLookupReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyLookupReply message"""
        pos = 0
        request_id, result = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            (db_content, pos) = database_content.unmarshall(data, pos)
            return DatabaseKeyLookupReply(
                request_id, result, database_content=db_content
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return DatabaseKeyLookupReply(
            request_id, result, "", error_message=error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result 
        )

        if self.result == 0:
            db_content = database_content.marshall(self.database_content)
            packed_error_message = ""
        else:
            db_content = ""
            packed_error_message = marshall_string(self.error_message)

        return "".join(
            [header, db_content, packed_error_message, ]
        )

