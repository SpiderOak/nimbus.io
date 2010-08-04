# -*- coding: utf-8 -*-
"""
stat_reply.py

StatReply message
"""
import struct
from collections import namedtuple

from diyapi_tools.marshalling import marshall_string, unmarshall_string

_header_tuple = namedtuple("Header", [ 
    "request_id",
    "result",
    "timestamp",
    "total_size",
    "file_adler",
    "file_md5",
    "userid",
    "groupid",
    "permissions"
])

# 32s - request-id 32 char hex uuid
# B   - result: 0 = success
# d - timestamp
# Q - total_size
# l - file_adler32
#16s- file_md5
# H - userid
# H - groupid
# H - permissions

_header_format = "!32sBdQl16sHHH"
_header_size = struct.calcsize(_header_format)

class StatReply(object):
    """AMQP message to reply with stat information"""

    routing_tag = "stat_reply"

    successful = 0
    error_database_failure = 1
    error_no_such_key = 2

    def __init__(
        self,
        request_id,
        result,
        timestamp=0.0,
        total_size=0,
        file_adler=0,
        file_md5="",
        userid=0,
        groupid=0,
        permissions=0,
        error_message=""
    ):
        self.request_id = request_id
        self.result = result
        self.timestamp = timestamp
        self.total_size = total_size
        self.file_adler = file_adler
        self.file_md5 = file_md5
        self.userid = userid
        self.groupid = groupid
        self.permissions = permissions
        self.error_message = error_message

    @property
    def error(self):
        return self.result != StatReply.successful

    @classmethod
    def unmarshall(cls, data):
        """return a StatReply message"""
        pos = 0
        header = _header_tuple._make(struct.unpack(
            _header_format, data[pos:pos+_header_size]
        ))
        pos += _header_size

        if header.result == 0:
            return StatReply(
                header.request_id,
                header.result,
                header.timestamp,
                header.total_size,
                header.file_adler,
                header.file_md5,
                header.userid,
                header.groupid,
                header.permissions
            )

        (error_message, pos) = unmarshall_string(data, pos)

        return StatReply(
            request_id,
            result,
            error_message
        )

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format,
            self.request_id,
            self.result,
            self.timestamp,
            self.total_size,
            self.file_adler,
            self.file_md5,
            self.userid,
            self.groupid,
            self.permissions
        )
        packed_error_message = marshall_string(self.error_message)
        return "".join([header, packed_error_message, ])

