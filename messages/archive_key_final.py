# -*- coding: utf-8 -*-
"""
archive_key_final.py

ArchiveKeyFinal message
"""
from collections import namedtuple
import struct

_header_tuple = namedtuple("Header", [ 
    "request_id", 
    "sequence",
    "total_size",
    "file_adler32",
    "file_md5",
    "segment_adler32",
    "segment_md5"
])

# 32s - request-id 32 char hex uuid
# I   - sequence
# Q   - total_size
# l   - file_adler32
# 16s - file_md5
# l   - segment_adler32
# 16s - segment_md5
_header_format = "32sIQl16sl16s"
_header_size = struct.calcsize(_header_format)

class ArchiveKeyFinal(object):
    """AMQP message to finish archiving a key"""

    routing_key = "data_writer.archive_key_final"

    def __init__(
        self, 
        request_id, 
        sequence, 
        total_size, 
        file_adler32, 
        file_md5, 
        segment_adler32, 
        segment_md5, 
        content
    ):
        self.request_id = request_id
        self.sequence = sequence
        self.total_size = total_size
        self.file_adler32 = file_adler32
        self.file_md5 = file_md5
        self.segment_adler32 = segment_adler32
        self.segment_md5 = segment_md5
        self.data_content = content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyStart message"""
        pos = 0
        header = _header_tuple._make(struct.unpack(
            _header_format, data[pos:pos+_header_size]
        ))
        pos += _header_size
        data_content = data[pos:]
        return ArchiveKeyFinal(
            header.request_id, 
            header.sequence, 
            header.total_size,
            header.file_adler32, 
            header.file_md5, 
            header.segment_adler32, 
            header.segment_md5, 
            data_content
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.sequence,
            self.total_size,
            self.file_adler32,
            self.file_md5,
            self.segment_adler32,
            self.segment_md5
        )
        return "".join([ header, self.data_content ])

