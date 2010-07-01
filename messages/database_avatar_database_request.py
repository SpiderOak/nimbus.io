# -*- coding: utf-8 -*-
"""
database_avatar_database_request.py

DatabaseAvatarDatabaseRequest message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   -  avatar_id
_header_format = "32sQ"
_header_size = struct.calcsize(_header_format)

class DatabaseAvatarDatabaseRequest(object):
    """
    AMQP message to request a list of known avatars from the database server
    """

    routing_key = "database_server.avatar_database_request"

    def __init__(
        self, 
        request_id, 
        avatar_id,
        dest_host,
        dest_dir,
        reply_exchange,
        reply_routing_header
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.dest_host = dest_host
        self.dest_dir = dest_dir
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseAvatarDatabaseRequest message"""
        pos = 0
        (request_id, avatar_id, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (dest_host, pos) = unmarshall_string(data, pos)
        (dest_dir, pos) = unmarshall_string(data, pos)
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        return DatabaseAvatarDatabaseRequest(
            request_id, 
            avatar_id,
            dest_host,
            dest_dir,
            reply_exchange, 
            reply_routing_header
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(_header_format, self.request_id, self.avatar_id)
        packed_dest_host = marshall_string(self.dest_host)
        packed_dest_dir = marshall_string(self.dest_dir)
        packed_reply_exchange = marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(
            self.reply_routing_header
        )
        return "".join([
            header, 
            packed_dest_host,
            packed_dest_dir,
            packed_reply_exchange, 
            packed_reply_routing_header
        ])

