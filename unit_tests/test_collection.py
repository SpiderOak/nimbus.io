# -*- coding: utf-8 -*-
"""
test_collection.py

test collections
"""
import unittest

from lumberyard.http_util import compute_default_hostname

from tools.standard_logging import initialize_logging
from tools.database_connection import get_central_connection
from tools.customer import purge_customer, \
        create_customer, \
        add_key_to_customer

from tools.collection import get_collection_from_hostname

_log_path = "%s/test_collection.log" % (os.environ["NIMBUSIO_LOG_DIR"], )
_cluster_name = "multi-node-cluster"
_local_node_name = "multi-node-01"
_test_username = "test-collection-user"

class TestCollections(unittest.TestCase):
    """test collections"""

    def setUp(self):
        self.tearDown()
        self._connection = get_central_connection()
        self._connection.execute("begin")
        purge_customer(self._connection, _test_username)
        create_customer(self._connection, _test_username)
        add_key_to_customer(self._connection, _test_username)
        self._connection.commit()

    def tearDown(self):
        if hasattr(self, "_connection") \
        and self._connection is not None:
            self._connection.execute("begin")
            purge_customer(self._connection, _test_username)
            self._connection.commit()
            self._connection.close()
            self._connection = None

    def test_default_hostname(self):
        """test getting the collection from a customer's default hostname"""
        hostname = compute_default_hostname(_test_username)
        collection_entry = get_collection_from_hostname(
            self._connection, hostname
        )
        self.assertEqual(collection_entry.username, _test_username)

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

