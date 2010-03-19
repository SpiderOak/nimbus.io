# -*- coding: utf-8 -*-
"""
archive_key_entire.py

ArchiveKeyEntire message
"""
import struct

class ArchiveKeyEntire(object):
    """
    AMQP message to archive a key wiht the entire content contained in this
    message
    """
    routing_key = "data_writer.archive_key_entire"

    def __init__( 
        self, 
        request_id, 
        avatar_id, 
        key, 
        timestamp, 
        segment, 
        adler32, 
        md5, 
        content
    ):
        self.request_id = request_id
        self.avatar_id = avatar_id
        self.key = key
        self.begin_timestamp = timestamp
        self.segment = segment
        self.adler32 = adler32
        self.md5 = md5
        self.content = content

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyEntire message"""

    def marshall(self):
        """return a data string suitable for transmission"""


