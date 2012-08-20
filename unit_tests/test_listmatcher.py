# -*- coding: utf-8 -*-
"""
test_listmatcher.py

test the web server's listmatch module
"""
import os
import sys
from datetime import timedelta
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.database_connection import get_node_local_connection
from tools.data_definition import create_timestamp
from web_public_reader.listmatcher import listmatch 

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
    connection.begin_trqansaction()
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
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 0, result)
        self.assertFalse(result["truncated"], result)

    def test_exact_match(self):
        """test getting a single key"""
        keys = [u"aaa", u"bbb", u"ccc", ]
        _load_test_data(self._connection, keys)
        result = listmatch(self._connection, _collection_id, prefix=keys[0])
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 1, result)
        self.assertFalse(result["truncated"], result)
        self.assertEqual(result["keys"][0], keys[0])

    def test_max_keys(self):
        """test cutting off the query at max_keys"""
        keys = [u"test-key1", u"test_key2", u"test_key3", ]
        _load_test_data(self._connection, keys)
        result = listmatch(
            self._connection, _collection_id, max_keys=len(keys)-1
        ) 
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), len(keys)-1, result)
        self.assertTrue(result["truncated"], result)

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
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 7, result)
        self.assertFalse(result["truncated"], result)
        for key in result["keys"]:
            self.assertTrue(key.startswith(test_prefix))

        result = listmatch(self._connection, _collection_id, prefix=u"aaa/b")
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 6, result)
        self.assertFalse(result["truncated"], result)

        result = listmatch(
            self._connection, _collection_id, prefix=u"aaa/b/ccccccccc/"
        )
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 3, result)
        self.assertFalse(result["truncated"], result)

        result = listmatch(
            self._connection, _collection_id, prefix=u"aaa/b/dddd"
        )
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 2)
        self.assertFalse(result["truncated"], result)

        result = listmatch(self._connection, _collection_id, prefix=u"aaa/e")
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 1)
        self.assertFalse(result["truncated"], result)

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
        self.assertFalse("keys" in result, result)
        self.assertTrue("prefixes" in result, result)
        result_set = set(result["prefixes"])
        self.assertEqual(result_set, set([u"aaa/", u"fff/"]))

        result = listmatch(
            self._connection, _collection_id, prefix=u"aaa/", delimiter=u"/"
        )
        self.assertFalse("keys" in result, result)
        self.assertTrue("prefixes" in result, result)
        result_set = set(result["prefixes"])
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
        self.assertTrue("keys" in result1, result1)
        self.assertTrue(len(result1["keys"]) <= cutoff, result1)
        self.assertTrue(result1["truncated"], result1)

        result2 = listmatch(
            self._connection, _collection_id, marker=result1["keys"][-1]
        )
        self.assertTrue("keys" in result2, result2)
        self.assertFalse(result2["truncated"], result2)

        self.assertEqual(
            len(result1["keys"]) + len(result2["keys"]), 
            len(keys),
            (result1, result2, )
        )

    def test_tombstone(self):
        """
        test finding the most recent of multiple rows when it is a tombstone
        """
        test_key = u"test_key"
        data_time = create_timestamp()
        tombstone_time = data_time + timedelta(hours=1)
        self._connection.execute("begin")
        self._connection.execute(_insert_test_row_with_timestamp_and_tombstone, 
                           [_collection_id, test_key, data_time, 42, False])
        self._connection.execute(_insert_test_row_with_timestamp_and_tombstone, 
                           [_collection_id, test_key, tombstone_time, 0, True])
        self._connection.commit()
        result = listmatch(self._connection, _collection_id)
        self.assertTrue("keys" in result, result)
        self.assertEqual(len(result["keys"]), 0)
        self.assertFalse(result["truncated"], result)

if __name__ == "__main__":
    unittest.main()

