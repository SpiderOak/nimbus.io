# -*- coding: utf-8 -*-
"""
database_listmatch_reply.py

DatabaseListMatchReply message
"""
import struct

from tools.marshalling import marshall_string, unmarshall_string

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success 
# ?   - is_complete
# I   - key count
_header_format = "!32sB?I"
_header_size = struct.calcsize(_header_format)

class DatabaseListMatchReply(object):
    """AMQP message to reply with a (partial) list of keys"""

    routing_tag = "database_key_lookup_reply"
   
    successful = 0
    error_database_failure = 2

    def __init__(
        self, 
        request_id,
        result,
        is_complete,
        key_list,
    ):
        self.request_id = request_id
        self.result = result
        self.is_complete = is_complete
        self.key_list = key_list

    @property
    def error(self):
        return self.result != DatabaseListMatchReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseKeyLookup message"""
        pos = 0
        (request_id, result, is_complete, list_size, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        key_list = list()
        for _ in xrange(list_size):
            (entry, pos) = unmarshall_string(data, pos)
            key_list.append(entry)

        return DatabaseListMatchReply(
            request_id, 
            result,
            is_complete, 
            key_list
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id, 
            self.result, 
            self.is_complete,
            len(self.key_list)
        )
        packed_keys = "".join([marshall_string(key) for key in self.key_list])
        return "".join([header, packed_keys, ])


