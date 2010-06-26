# -*- coding: utf-8 -*-
"""
anti_entropy_request.py

AntiEntropyAuditRequest message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# Q   - avatar_id
_header_format = "32sQ"
_header_size = struct.calcsize(_header_format)

class AntiEntropyAuditRequest(object):
    """
    AMQP message to request anti_entropy server to run an audit on an avatar
    """

    routing_key = "anti-entropy.audit_request"

    def __init__(
        self, 
        request_id, 
        avatar_id,
        reply_exchange,
        reply_routing_header
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.reply_exchange = reply_exchange
        self.reply_routing_header = reply_routing_header

    @classmethod
    def unmarshall(cls, data):
        """return a AntiEntropyAuditRequest message"""
        pos = 0
        (request_id, avatar_id, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (reply_exchange, pos) = unmarshall_string(data, pos)
        (reply_routing_header, pos) = unmarshall_string(data, pos)
        return AntiEntropyAuditRequest(
            request_id, 
            avatar_id,
            reply_exchange, 
            reply_routing_header
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(_header_format, self.request_id, self.avatar_id, )
        packed_reply_exchange = marshall_string(self.reply_exchange)
        packed_reply_routing_header = marshall_string(
            self.reply_routing_header
        )
        return "".join([
            header, 
            packed_reply_exchange, 
            packed_reply_routing_header
        ])

