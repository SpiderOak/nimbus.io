# -*- coding: utf-8 -*-
"""
database_avatar_list_reply.py

DatabaseAvatarListReply message
"""
import struct


# 32s - request-id 32 char hex uuid
_header_format = "32s"
_header_size = struct.calcsize(_header_format)

_avatar_id_format = "I"
_avatar_id_size =  struct.calcsize(_avatar_id_format)

class DatabaseAvatarListReply(object):
    """AMQP message returning a list of known avatars from the database"""

    routing_tag = "database_avatar_list_reply"
   
    def __init__(self, request_id):
        self.request_id = request_id
        self._avatar_ids = None

    @classmethod
    def unmarshall(cls, data):
        """return a DatabaseAvatarList message"""
        pos = 0
        (request_id, ) = struct.unpack(
            _header_format, data[pos:pos+_header_size]
        )
        pos += _header_size

        message = DatabaseAvatarListReply(request_id)
        message._avatar_ids = data[pos:] 
        return message

    def marshall(self):
        """return a data string suitable for transmission"""
        header = struct.pack(
            _header_format, 
            self.request_id
        )

        return "".join([header, self._avatar_ids, ])

    def put(self, avatar_id_iter):
        """build the avatar_id data from an iterator"""
        avatar_id_list = [
            struct.pack(_avatar_id_format, a) for a in avatar_id_iter
        ]
        self._avatar_ids = "".join(avatar_id_list)

    def get(self):
        """generate individual avatar_ids from interanl data"""
        i = 0
        while i < len(self._avatar_ids):
            (avatar_id, ) =  struct.unpack(
                _avatar_id_format, self._avatar_ids[i:i+_avatar_id_size]
            )
            yield avatar_id
            i += _avatar_id_size

