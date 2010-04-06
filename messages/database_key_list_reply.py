# -*- coding: utf-8 -*-
"""
database_key_list_reply.py

DatabaseKeyListReply message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string
from diyapi_database_server import database_content

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
# I   - content list size
_header_format = "!32sBI"
_header_size = struct.calcsize(_header_format)

class DatabaseKeyListReply(object):
    """AMQP message to insert a key in the database"""

    routing_tag = "database_key_list_reply"
   
    successful = 0
    error_unknown_key = 1
    error_database_failure = 2

    def __init__(
        self, request_id, result, content_list=[], error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.content_list = content_list
        self.error_message = error_message

    @property
    def error(self):
        return self.result != DatabaseKeyListReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyListReply message"""
        pos = 0
        request_id, result, content_list_size = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        if result == 0:
            content_list = list()
            for _ in range(content_list_size):
                (db_content, pos) = database_content.unmarshall(data, pos)
                content_list.append(db_content)
            return DatabaseKeyListReply(
                request_id, result, content_list=content_list
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return DatabaseKeyListReply(
            request_id, result, "", error_message=error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result,
            len(self.content_list)
        )
        output_list = [header, ]
        for db_content in self.content_list:
            output_list.append(database_content.marshall(db_content))
        output_list.append(marshall_string(self.error_message))

        return "".join(output_list)

