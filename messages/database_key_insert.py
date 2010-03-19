# -*- coding: utf-8 -*-
"""
database_key_insert.py

DatabaseKeyInsert message
"""
import struct

from diyapi_database_server import database_content

_header_format = "!32sI" # key size
_header_size = struct.calcsize(_header_format)
_key_format = "%ds"

class DatabaseKeyInsert(object):
    """AMQP message to insert a key in the database"""

    routing_key = "database.key_insert"

    def __init__(self, request_id, key, content):
        self.request_id = request_id
        self.key = key
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyInsert message"""
        pos = 0
        (request_id, key_size, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (key, ) = struct.unpack(
            _key_format % (key_size, ), data[pos:pos+key_size]
        )
        pos += key_size
        content = database_content.unmarshall(data[pos:])
        return DatabaseKeyInsert(request_id, key, content)

    def marshall(self):
        """return a data string suitable for transmission"""
        key_size = len(self.key)
        header = struct.pack(_header_format, self.request_id, key_size)
        packed_key = struct.pack(_key_format % (key_size, ), self.key)
        return "".join(
            [header, packed_key, database_content.marshall(self.content)]
        )

