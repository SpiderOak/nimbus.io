# -*- coding: utf-8 -*-
"""
test_sql_authenticator.py

test the customer authentication process
as a part of the test, it give the customer rputines a good workout too
"""
import unittest

from lumberyard.http_util import compute_authentication_string, \
    current_timestamp

from tools.standard_logging import initialize_logging
from tools.database_connection import get_central_connection
from tools.customer import purge_customer, \
        create_customer, \
        add_key_to_customer, \
        list_customer_keys

from web_server.sql_authenticator import SqlAuthenticator

class MockRequest(object):
    pass

_log_path = "%s/test_sql_authenticator.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], 
)
_cluster_name = "multi-node-cluster"
_local_node_name = "multi-node-01"
_test_username = "test-sqlauthenticator-user"
_test_method = "POST"
_test_uri = "/list_collections"

class TestSqlAuthenticator(unittest.TestCase):
    """test the customer authentication process"""

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

    def test_valid_customer(self):
        """test a customer who should authenticate successfully"""
        request = MockRequest()

        key_list = list_customer_keys(self._connection, _test_username)
        self.assertEqual(len(key_list), 1)
        key_id, key_value = key_list[0]

        authentication_string = compute_authentication_string(
            key_id,
            key_value,
            _test_username,
            _test_method,
            current_timestamp(),
            _test_uri
        )
        request.__dict__["authorization"] = authentication_string.split()
        request.__dict__["method"] = _test_method
        request.__dict__["headers"] = {
            'x-nimbus-io-timestamp' : str(current_timestamp())
        } 
        request.__dict__["path_qs"] = _test_uri

        authenticator = SqlAuthenticator()
        authenticated = authenticator.authenticate(
            self._connection, _test_username, request
        )
        self.assertTrue(authenticated)

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

