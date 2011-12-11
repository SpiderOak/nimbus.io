# -*- coding: utf-8 -*-
"""
test_listmatcher.py

test the web server's listmatch module
"""
import os
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.database_connection import get_node_local_connection
from web_server.listmatcher import listmatch 

_node_name = "multi-node-01"
os.environ["NIMBUSIO_NODE_NAME"] = _node_name
os.environ["NIMBUSIO_NODE_USER_PASSWORD"] = ""
_collection_id = 0

def _clear_test_data(connection):
    connection.execute(
        "delete from nimbusio_node.segment where collection_id = %s",
        [_collection_id, ]
    )
    connection.commit()

_test_keys = [
    "aaa",
]

_insert_test_row = """
    insert into nimbusio_node.segment 
    (collection_id, key, timestamp, segment_num, file_size, file_adler32, 
    file_hash)
    values (%s, %s, current_timestamp, 0, 42, 0, 'aaaaaaaaaaaaaaaa');
"""

def _load_test_data(connection):
    connection.execute("begin")
    for test_key in _test_keys:
        connection.execute(_insert_test_row, [_collection_id, test_key])
    connection.commit()

class TestListMatcher(unittest.TestCase):
    """
    test the web server's listmatch module
    """

    def setUp(self):
        self._connection = get_node_local_connection()
        _clear_test_data(self._connection)
        _load_test_data(self._connection)

    def tearDown(self):
        _clear_test_data(self._connection)
        self._connection.close()

    def test_empty_collection(self):
        """test querying an empty collection"""
        _clear_test_data(self._connection)
        result = listmatch(self._connection, _collection_id, "")
        self.assertEqual(len(result), 0, result)

    def test_exact_match(self):
        """test getting a single key"""
        test_key = "aaa"
        result = listmatch(self._connection, _collection_id, test_key)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], test_key)

if __name__ == "__main__":
    unittest.main()

