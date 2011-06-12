# -*- coding: utf-8 -*-
"""
test_read_and_write.py

test writing and reading back 
"""
from collections import namedtuple
from datetime import datetime
import hashlib
import os
import os.path
import shutil
import time
import unittest

import psycopg2

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.pandora_database_connection import get_node_local_connection

from diyapi_web_server.data_slicer import DataSlicer
from diyapi_web_server.zfec_segmenter import ZfecSegmenter

_log_path = "/var/log/pandora/test_read_and_write.log"
_test_dir = os.path.join("/tmp", "test_read_and_write")
_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]

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
    return key_row_id

def _retrieve_key_rows(connection, avatar_id, key):
    """
    retrieve all rows for avatar-id and key.
    Note that there is no unique constraint on (avatar_id, key):
    the caller must be prepared to deal with multiple rows
    """
    result = connection.fetch_all_rows("""
        select %s from diy.key 
        where avatar_id = %%s and name = %%s
        order by timestamp  
    """ % (",".join(key_row_template._fields), ), [avatar_id, key, ])
    print result
    return [key_row_template._make(row) for row in result]

class TestReadAndWrite(unittest.TestCase):
    """test writing and reading back"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_test_dir)

        self._database_connection = get_node_local_connection()

    def tearDown(self):
        if hasattr(self, "_database_connection") \
        and self._database_connection is not None:
            self._database_connection.close()
            self._database_connection = None

        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_key_table(self):
        """test simple insert into, select from and delete of key table"""
        avatar_id = 1001
        key = "pork"
        md5 = hashlib.md5()
        key_row = key_row_template(
            name=key,
            id=None,
            avatar_id=avatar_id,
            timestamp=datetime.fromtimestamp(time.time()),
            size=42,
            adler32=42,
            hash=psycopg2.Binary(md5.digest()),
            user_id=0,
            group_id=0,
            permissions=0,
            tombstone=False,
            segment_num=0,
            handoff_node_id=None
        )
        key_row_id = _insert_key_row(self._database_connection, key_row)

        self.assertNotEqual(key_row_id, None)
        retrieved_rows = _retrieve_key_rows(
            self._database_connection, avatar_id, key
        )
        self.assertEqual(len(retrieved_rows), 1)

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

