# -*- coding: utf-8 -*-
"""
test_shard_id_factory.py
"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.database_connection import get_node_local_connection
from tools.shard_id_factory import ShardIDFactory

class TestShardIDFactory(unittest.TestCase):
    """test the customer authentication process"""

    def setUp(self):
        self.tearDown()
        self._connection = get_node_local_connection()

    def tearDown(self):
        if hasattr(self, "_connection") \
        and self._connection is not None:
            self._connection.close()
            self._connection = None

    def test_increasing_ids(self):
        """test that shard ids increase"""
        shard_id_factory = ShardIDFactory(self._connection, 1)
        prev_id = None
        for _ in range(1000):
            shard_id = shard_id_factory.next()
            if prev_id is not None:
                self.assertTrue(shard_id > prev_id)
            prev_id = shard_id

if __name__ == "__main__":
    unittest.main()

