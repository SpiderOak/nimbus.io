# -*- coding: utf-8 -*-
"""
space_accounting_detail.py

SpaceAccountingDetail message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# Q - avatar_id
# d - timestamp
# I - value 
_header_format = "QdI"
_header_size = struct.calcsize(_header_format)

class SpaceAccountingDetail(object):
    """
    AMQP message to report space accounting information for an avatar
    """
    bytes_added = "bytes_added"
    bytes_retrieved = "bytes_retrieved"
    bytes_removed = "bytes_removed"

    routing_key = "space-accounting.detail"

    def __init__(self, avatar_id, timestamp, event, value):
        self.avatar_id = avatar_id
        self.timestamp = timestamp
        self.event = event
        self.value = value

    @classmethod
    def unmarshall(cls, data):
        """return a SpaceAccountingDetail message"""
        pos = 0
        (avatar_id, timestamp, value, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (event, pos) = unmarshall_string(data, pos)

        return SpaceAccountingDetail(
            avatar_id,
            timestamp,
            event,
            value
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, self.avatar_id, self.timestamp, self.value
        )

        packed_event =  marshall_string(self.event)

        return "".join([header, packed_event, ])

