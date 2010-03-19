# -*- coding: utf-8 -*-
"""
archive_key_start.py

ArchiveKeyStart message
"""
import struct

class ArchiveKeyStart(object):
    """AMQP message to start archiving a key"""

    routing_key = "data_writer.archive_key_start"

    def __init__(
        self, request_id, avatar_id, key, timestamp, sequence, segment, content
    ):
        self.request_id = request_id
        self._avatar_id = avatar_id
        self.key = key
        self.begin_timestamp = timestamp
        self.sequence = sequence
        self.segment = segment
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyStart message"""

    def marshall(self):
        """return a data string suitable for transmission"""

