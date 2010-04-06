# -*- coding: utf-8 -*-
"""
process_status.py

ProcessStatus message
"""
import struct

from diyapi_tools.marshalling import marshall_string, unmarshall_string

# d   - timestamp
_header_format = "d"
_header_size = struct.calcsize(_header_format)

class ProcessStatus(object):
    """
    AMQP message to broadcast a process's status to the world
    """
    status_startup = "startup"
    status_shutdown = "shutdown"

    routing_key = "process_status"

    def __init__(self, timestamp, exchange, routing_header, status):
        self.timestamp = timestamp
        self.exchange = exchange
        self.routing_header = routing_header
        self.status = status

    @classmethod
    def unmarshall(cls, data):
        """return a ArchiveKeyEntire message"""
        pos = 0
        (timestamp, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size
        (exchange, pos) = unmarshall_string(data, pos)
        (routing_header, pos) = unmarshall_string(data, pos)
        (status, pos) = unmarshall_string(data, pos)
        return ProcessStatus(
            timestamp,
            exchange,
            routing_header,
            status
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(_header_format, self.timestamp)

        packed_exchange =  marshall_string(self.exchange)
        packed_routing_header = marshall_string(self.routing_header)
        packed_status = marshall_string(self.status)
        return "".join([
                header,
                packed_exchange,
                packed_routing_header,
                packed_status
            ])

