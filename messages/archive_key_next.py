# -*- coding: utf-8 -*-
"""
archive_key_next.py

ArchiveKeyNext message
"""
import struct

# 32s - request-id 32 char hex uuid
# I   - sequence
_header_format = "32sI"
_header_size = struct.calcsize(_header_format)

class ArchiveKeyNext(object):
    """AMQP message to continue archiving a key"""

    routing_key = "data_writer.archive_key_next"

    def __init__(self, request_id, sequence, data_content):
        self.request_id = request_id
        self.sequence = sequence
        self.data_content = data_content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyNext message"""
        pos = 0
        request_id, sequence = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        data_content = data[pos:]
        return ArchiveKeyNext(request_id, sequence, data_content)

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(_header_format, self.request_id, self.sequence)
        return "".join([header, self.data_content, ])

