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

from unit_tests.web_server import util

from diyapi_web_server.sql_authenticator import SqlAuthenticator


_test_username = 'test_username'
_test_key_id = 1001
_test_key = 'deadbeef'


class TestSqlAuthenticator(unittest.TestCase):
    """test diyapi_web_server/sql_authenticator.py"""
    def setUp(self):
        self.req = Request.blank('/')
        self.connection = util.MockSqlConnection()
        self.connection.cursor().rows = {
            ('select key '
             'from diy_key '
             'where key_id=%s', (_test_key_id,)): [(_test_key,)],
            ('select key_id '
             'from diy_user '
             'join diy_user_key using (user_id) '
             'where username=%s', (_test_username,)): [(_test_key_id,)],
        }
        self.authenticator = SqlAuthenticator(self.connection)
        self._real_time = time.time
        time.time = util.fake_time

    def tearDown(self):
        time.time = self._real_time

    def test_fails_when_header_is_missing(self):
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_fails_for_incorrect_auth_type(self):
        self.req.authorization = 'Basic asdfegkesj=='
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_succeeds(self):
        timestamp = int(util.fake_time())
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(
            _test_key,
            string_to_sign,
            hashlib.sha256
        ).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (_test_key_id, signature)
        self.assertTrue(self.authenticator.authenticate(self.req))
        self.assertEqual(self.req.remote_user, _test_key_id)

    def test_fails_for_nonexistent_key_id(self):
        timestamp = int(util.fake_time())
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(
            _test_key,
            string_to_sign,
            hashlib.sha256
        ).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (_test_key_id + 1, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_succeeds_if_timestamp_within_10_minutes(self):
        timestamp = int(util.fake_time()) + 10 * 60
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(
            _test_key,
            string_to_sign,
            hashlib.sha256
        ).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (_test_key_id, signature)
        self.assertTrue(self.authenticator.authenticate(self.req))
        self.assertEqual(self.req.remote_user, _test_key_id)

    def test_fails_if_missing_timestamp(self):
        timestamp = int(util.fake_time())
        string_to_sign = '\n'.join((
            self.req.method,
            str(timestamp),
        ))
        signature = hmac.new(
            _test_key,
            string_to_sign,
            hashlib.sha256
        ).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (_test_key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_fails_if_timestamp_skewed(self):
        timestamp = int(util.fake_time()) + 10 * 60 + 1
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        string_to_sign = '\n'.join((
            self.req.method,
            self.req.headers['x-diyapi-timestamp'],
        ))
        signature = hmac.new(
            _test_key,
            string_to_sign,
            hashlib.sha256
        ).hexdigest()
        self.req.authorization = 'DIYAPI %d:%s' % (_test_key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))

    def test_fails_if_signature_mismatched(self):
        timestamp = int(util.fake_time())
        self.req.headers['x-diyapi-timestamp'] = str(timestamp)
        signature = 'bogussignature'
        self.req.authorization = 'DIYAPI %d:%s' % (_test_key_id, signature)
        self.assertFalse(self.authenticator.authenticate(self.req))


if __name__ == "__main__":
    unittest.main()
