# -*- coding: utf-8 -*-
"""
test_unified_id_factory.py
"""
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.database_connection import get_node_local_connection
from tools.unified_id_factory import UnifiedIDFactory

class TestUnifiedIDFactory(unittest.TestCase):
    """test the unified id factory"""

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
        unified_id_factory = UnifiedIDFactory(self._connection, 1)
        prev_id = None
        for _ in range(1000):
            unified_id = unified_id_factory.next()
            if prev_id is not None:
                self.assertTrue(unified_id > prev_id)
            prev_id = unified_id

if __name__ == "__main__":
    unittest.main()

