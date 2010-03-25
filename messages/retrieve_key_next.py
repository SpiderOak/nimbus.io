# -*- coding: utf-8 -*-
"""
retrieve_key_next.py

RetrieveKeyNext message
"""
import struct

# 32s - request-id 32 char hex uuid
# I   - sequence
_header_format = "32sI"
_header_size = struct.calcsize(_header_format)

class RetrieveKeyNext(object):
    """
    AMQP message to start retrieving a key 
    """
    routing_key = "data_writer.archive_key_next"

    def __init__(self, request_id, sequence):
        self.request_id = request_id
        self.sequence = sequence

    @classmethod
    def unmarshall(cls, data):
        """return a RetrieveKeyNext message"""
        pos = 0
        request_id, sequence = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        return RetrieveKeyNext(request_id, sequence)

    def marshall(self):
        """return a data string suitable for transmission"""
        return struct.pack(
            _header_format,
            self.request_id,
            self.sequence
        )
