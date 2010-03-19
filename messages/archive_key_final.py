# -*- coding: utf-8 -*-
"""
archive_key_final.py

ArchiveKeyFinal message
"""
import struct

class ArchiveKeyFinal(object):
    """AMQP message to finish archiving a key"""

    routing_key = "data_writer.archive_key_final"

    def __init__(
        self, content_id, sequence, segment, total_size, adler32, md5, content
    ):
        self.content_id = content_id
        self.sequence = sequence
        self.segment = segment
        self.total_size = total_size
        self.adler32 = adler32
        self.md5 = md5
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyStart message"""

    def marshall(self):
        """return a data string suitable for transmission"""


