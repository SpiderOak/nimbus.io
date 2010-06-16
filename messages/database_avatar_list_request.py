# -*- coding: utf-8 -*-
"""
database_avatar_list_request.py

DatabaseAvatarListRequest message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
_header_format = "32s"
_header_size = struct.calcsize(_header_format)

class DatabaseAvatarListRequest(object):
    """
    AMQP message to request a list of known avatars from the database server
    """

    routing_key = "database_server.avatar_list_request"

    def __init__(
        self, 
        request_id, 
        reply_exchange,
        reply_routing_header
    ):
        self.request_id = request_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseAvatarListRequest message"""
        pos = 0
        (request_id, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        return DatabaseAvatarListRequest(
            request_id, 
            reply_exchange, 
            reply_routing_header
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(_header_format, self.request_id)
        packed_reply_exchange = marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(
            self.reply_routing_header
        )
        return "".join([
            header, packed_reply_exchange, packed_reply_routing_header
        ])

