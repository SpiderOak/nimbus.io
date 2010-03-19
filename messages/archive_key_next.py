# -*- coding: utf-8 -*-
"""
archive_key_next.py

ArchiveKeyNext message
"""
import struct

class ArchiveKeyNext(object):
    """AMQP message to continue archiving a key"""

    routing_key = "data_writer.archive_key_next"

    def __init__(self, request_id, sequence, segment, content):
        self.request_id = request_id
        self.sequence = sequence
        self.segment = segment
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyNext message"""

    def marshall(self):
        """return a data string suitable for transmission"""


