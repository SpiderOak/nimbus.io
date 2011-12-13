# -*- coding: utf-8 -*-
"""
test_listmatcher.py

test the web server's listmatch module
"""
import os
import sys
from datetime import datetime, timedelta
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

_insert_test_row = """
    insert into nimbusio_node.segment 
    (collection_id, key, timestamp, segment_num, file_size, file_adler32, 
    file_hash)
    values (%s, %s, current_timestamp, 0, 42, 0, 'aaaaaaaaaaaaaaaa');
"""

_insert_test_row_with_timestamp_and_tombstone= """
    insert into nimbusio_node.segment 
    (collection_id, key, timestamp, segment_num, file_size, file_adler32, 
    file_hash, file_tombstone)
    values (%s, %s, %s::timestamp, 0, %s, 0, 'aaaaaaaaaaaaaaaa', %s);
"""

def _load_test_data(connection, keys):
    connection.execute("begin")
    for key in keys:
        connection.execute(_insert_test_row, [_collection_id, key])
    connection.commit()

class TestListMatcher(unittest.TestCase):
    """
    test the web server's listmatch module
    """

    def setUp(self):
        self._connection = get_node_local_connection()
        _clear_test_data(self._connection)

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
        keys = [u"aaa", u"bbb", u"ccc", ]
        _load_test_data(self._connection, keys)
        result = listmatch(self._connection, _collection_id, prefix=keys[0])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], keys[0])

    def test_max_keys(self):
        """test cutting off the query at max_keys"""
        keys = [u"test-key1", u"test_key2", u"test_key3", ]
        _load_test_data(self._connection, keys)
        result = listmatch(
            self._connection, _collection_id, max_keys=len(keys)-1
        ) 
        self.assertEqual(len(result), len(keys)-1)

    def test_tree(self):
        """test storing and retrieving a directory tree"""
        keys = [
            u"aaa/b/cccc/1", 
            u"aaa/b/ccccccccc/1", 
            u"aaa/b/ccccccccc/2", 
            u"aaa/b/ccccccccc/3", 
            u"aaa/b/dddd/1", 
            u"aaa/b/dddd/2", 
            u"aaa/e/ccccccccc/1", 
            u"fff/e/ccccccccc/1", 
        ]
        _load_test_data(self._connection, keys)

        test_prefix = u"aaa/"
        result = listmatch(
            self._connection, _collection_id, prefix=test_prefix
        )
        self.assertEqual(len(result), 7)
        for key in result:
            self.assertTrue(key.startswith(test_prefix))

        result = listmatch(self._connection, _collection_id, prefix=u"aaa/b")
        self.assertEqual(len(result), 6)

        result = listmatch(
            self._connection, _collection_id, prefix=u"aaa/b/ccccccccc/"
        )
        self.assertEqual(len(result), 3)

        result = listmatch(
            self._connection, _collection_id, prefix=u"aaa/b/dddd"
        )
        self.assertEqual(len(result), 2)

        result = listmatch(self._connection, _collection_id, prefix=u"aaa/e")
        self.assertEqual(len(result), 1)

    def test_delimiter(self):
        """test using a delimiter with a directory tree"""
        keys = [
            u"aaa",
            u"aaa/b/cccc/1", 
            u"aaa/b/ccccccccc/1", 
            u"aaa/b/ccccccccc/2", 
            u"aaa/b/ccccccccc/3", 
            u"aaa/b/dddd/1", 
            u"aaa/b/dddd/2", 
            u"aaa/e/ccccccccc/1", 
            u"fff/e/ccccccccc/1", 
        ]
        _load_test_data(self._connection, keys)

        result = listmatch(self._connection, _collection_id, delimiter=u"/")
        result_set = set(result)
        self.assertEqual(result_set, set([u"aaa/", u"fff/"]))

        result = listmatch(
            self._connection, _collection_id, prefix=u"aaa/", delimiter=u"/"
        )
        result_set = set(result)
        self.assertEqual(result_set, set([u"b/", u"e/"]), result_set)

    def test_marker(self):
        """test using a marker with a directory tree"""
        keys = [
            u"aaa/b/cccc/1", 
            u"aaa/b/ccccccccc/1", 
            u"aaa/b/ccccccccc/2", 
            u"aaa/b/ccccccccc/3", 
            u"aaa/b/dddd/1", 
            u"aaa/b/dddd/2", 
            u"aaa/e/ccccccccc/1", 
            u"fff/e/ccccccccc/1", 
        ]
        cutoff = len(keys) / 2
        _load_test_data(self._connection, keys)

        result1 = listmatch(self._connection, _collection_id, max_keys=cutoff)

        result2 = listmatch(
            self._connection, _collection_id, marker=result1[-1]
        )

        self.assertEqual(len(result1) + len(result2), len(keys))

    def test_tombstone(self):
        """
        test finding the most recent of multiple rows when it is a tombstone
        """
        test_key = u"test_key"
        data_time = datetime.now()
        tombstone_time = data_time + timedelta(hours=1)
        self._connection.execute("begin")
        self._connection.execute(_insert_test_row_with_timestamp_and_tombstone, 
                           [_collection_id, test_key, data_time, 42, False])
        self._connection.execute(_insert_test_row_with_timestamp_and_tombstone, 
                           [_collection_id, test_key, tombstone_time, 0, True])
        self._connection.commit()
        result = listmatch(self._connection, _collection_id)
        self.assertEqual(len(result), 0)

if __name__ == "__main__":
    unittest.main()

