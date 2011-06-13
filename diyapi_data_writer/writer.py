# -*- coding: utf-8 -*-
"""
writer.py

Manage writing key values to disk
"""
import logging

key_row_template = namedtuple(
    "KeyRow", [
        "name",
        "id",
        "avatar_id",
        "timestamp",
        "size",
        "adler32",
        "hash",
        "user_id",
        "group_id",
        "permissions",
        "tombstone",
        "segment_num",
        "handoff_node_id",
    ]
)

def _insert_key_row(connection, key_row):
    """
    Insert one key entry, returning the row id
    """
    cursor = connection._connection.cursor()
    cursor.execute("""
        insert into diy.key (
            name,
            avatar_id,
            timestamp,
            size,
            adler32,
            hash,
            user_id,
            group_id,
            permissions,
            tombstone,
            segment_num,
            handoff_node_id
        ) values (
            %(name)s,
            %(avatar_id)s,
            %(timestamp)s::timestamp,
            %(size)s,
            %(adler32)s,
            %(hash)s,
            %(user_id)s,
            %(group_id)s,
            %(permissions)s,
            %(tombstone)s,
            %(segment_num)s,
            %(handoff_node_id)s
        )
        returning id
    """, key_row._asdict())
    (key_row_id, ) = cursor.fetchone()
    cursor.close()
    connection.commit()
    return key_row_id

class Writer(object):
    """
    Manage writing key values to disk
    """
    def __init__(self, connection, repository_path):
        self._log = logging.getLogge("Writer")
        self._connection = connection
        self._repository_path = repository_path

        # open a new value file at startup

    def close(self):

    def start_new_key(self, avatar, key, segment_num):
        pass

    def store_key_sequence(self):
        pass

    def finish_new_key(self): 
#        md5 = hashlib.md5()
#        key_row = key_row_template(
#            name=key,
#            id=None,
#            avatar_id=avatar_id,
#            timestamp=datetime.fromtimestamp(time.time()),
#            size=42,
#            adler32=42,
#            hash=psycopg2.Binary(md5.digest()),
#            user_id=0,
#            group_id=0,
#            permissions=0,
#            tombstone=False,
#            segment_num=0,
#            handoff_node_id=None
#        )
#        key_row_id = _insert_key_row(self._database_connection, key_row)

        pass

    
