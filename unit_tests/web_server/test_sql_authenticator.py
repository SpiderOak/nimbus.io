# -*- coding: utf-8 -*-
"""
test_sql_authenticator.py

test diyapi_web_server/sql_authenticator.py
"""
import time
import hmac
import hashlib
import unittest

from webob import Request

from diyapi_web_server.sql_authenticator import SqlAuthenticator


class MockCursor(object):
    def __init__(self):
        self.rows = []
        self.queries = []

    def execute(self, query, args=()):
        self.queries.append((query, args))

    def fetchone(self):
        try:
            return self.rows[0]
        except IndexError:
            return None


class MockConnection(object):
    def __init__(self):
        self._cursor = MockCursor()

    def cursor(self):
        return self._cursor


def fake_time():
    return 12345


class TestSqlAuthenticator(unittest.TestCase):
    """test diyapi_web_server/sql_authenticator.py"""
    def setUp(self):
        self.req = Request.blank('/')
        self.connection = MockConnection()
        self.authenticator = SqlAuthenticator(self.connection)
        self._real_time = time.time
        time.time = fake_time

    def tearDown(self):
        time.time = self._real_time

    def test_fails_when_header_is_missing(self):
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_fails_for_incorrect_auth_type(self):
        self.req.authorization = 'Basic asdfegkesj=='
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_succeeds(self):
        key_id = 1001
        key = 'deadbeef'
        timestamp = fake_time()
        self.connection.cursor().rows.append((key,))
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (key_id, signature)
        self.assertTrue(self.authenticator.authenticate(self.req))
        self.assertEqual(self.req.remote_user, key_id)

    def test_fails_for_nonexistent_key_id(self):
        key_id = 1001
        key = 'deadbeef'
        timestamp = fake_time()
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_succeeds_if_timestamp_within_10_minutes(self):
        key_id = 1001
        key = 'deadbeef'
        timestamp = fake_time() + 10 * 60
        self.connection.cursor().rows.append((key,))
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (key_id, signature)
        self.assertTrue(self.authenticator.authenticate(self.req))
        self.assertEqual(self.req.remote_user, key_id)

    def test_fails_if_missing_timestamp(self):
        key_id = 1001
        key = 'deadbeef'
        timestamp = fake_time()
        self.connection.cursor().rows.append((key,))
        string_to_sign = '\n'.join((
            self.req.method,
            str(timestamp),
        ))
        signature = hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_fails_if_timestamp_skewed(self):
        key_id = 1001
        key = 'deadbeef'
        timestamp = fake_time() + 10 * 60 + 1
        self.connection.cursor().rows.append((key,))
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_fails_if_signature_mismatched(self):
        key_id = 1001
        key = 'deadbeef'
        timestamp = fake_time()
        self.connection.cursor().rows.append((key,))
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        signature = 'bogussignature'
        self.req.authorization = 'DIYAPI %d:%s' % (key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))


if __name__ == "__main__":
    unittest.main()
