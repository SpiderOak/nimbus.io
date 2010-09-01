# -*- coding: utf-8 -*-
"""
test_web_server.py

test diyapi_web_server/diyapi_web_server_main.py
"""
# importing this monkey-patches socket, putting us in geventland
from diyapi_web_server.diyapi_web_server_main import WebServer

import os
import re
import sys
import shutil
import logging
import unittest
import urllib2
import hmac
import hashlib
import zlib
import time
import json

from diyapi_tools.standard_logging import initialize_logging
from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_web_server.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["DIYAPI_REPOSITORY_PATH"] = _repository_path

_prod_base_url = os.environ['DIYAPI_TEST_BASE_URL']
_local_base_url = 'http://127.0.0.1:8088'

_test_username = os.environ['DIYAPI_TEST_USERNAME']
_test_key_id = int(os.environ['DIYAPI_TEST_KEY_ID'])
_test_key = os.environ['DIYAPI_TEST_KEY']


class TestWebServer(unittest.TestCase):
    """test diyapi_web_server/diyapi_web_server_main.py"""

    def setUp(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

        if 'prod' not in sys.argv[1:]:
            self.server = WebServer()
            self.server.start()

    def tearDown(self):
        if 'prod' not in sys.argv[1:]:
            self.server.stop()

    def _auth_headers(self, method, key=None):
        if key is None:
            key = _test_key
        timestamp = str(int(time.time()))
        string_to_sign = '\n'.join((
            _test_username,
            method,
            timestamp,
        ))
        signature = hmac.new(key, string_to_sign, hashlib.sha256).hexdigest()
        return {
            'Authorization': 'DIYAPI %d:%s' % (_test_key_id, signature),
            'X-DIYAPI-Timestamp': timestamp,
        }

    def _make_request(self, url, data=None, key=None, read_resp=True):
        request = urllib2.Request(url, data, self._auth_headers('GET' if data is None else 'POST', key))
        resp = urllib2.urlopen(request)
        if read_resp:
            return resp.read()
        return resp

    def test_unauthorized_when_auth_header_missing(self):
        log = logging.getLogger('test_unauthorized_when_auth_header_missing')
        log.info('start')
        try:
            resp = urllib2.urlopen(
                _base_url + '/data/test-key?action=listmatch')
        except urllib2.HTTPError, err:
            self.assertEqual(err.code, 401)
        else:
            raise AssertionError('was expecting a 401 but got %d: %r' % (resp.code, resp.read()))

    def test_unauthorized_with_bad_credentials(self):
        log = logging.getLogger('test_unauthorized_with_bad_credentials')
        log.info('start')
        try:
            resp = self._make_request(
                _base_url + '/data/test-key?action=listmatch', key='cafeface',
                read_resp=False)
        except urllib2.HTTPError, err:
            self.assertEqual(err.code, 401)
        else:
            raise AssertionError('was expecting a 401 but got %d: %r' % (resp.code, resp.read()))

    def test_upload_0_bytes(self):
        log = logging.getLogger('test_upload_0_bytes')
        log.info('start')
        content = ''
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        self.assertEqual(result, 'OK')

    def test_upload_0_bytes_and_listmatch(self):
        log = logging.getLogger('test_upload_0_bytes_and_listmatch')
        log.info('start')
        content = ''
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('listmatch')
        result = self._make_request(
            _base_url + '/data/test-key?action=listmatch')
        self.assertEqual(json.loads(result), [key])

    def test_upload_0_bytes_and_retrieve(self):
        log = logging.getLogger('test_upload_0_bytes_and_retrieve')
        log.info('start')
        content = ''
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('retrieve')
        result = self._make_request(
            _base_url + '/data/' + key)
        self.assertEqual(len(result), len(content))
        self.assertEqual(result, content)

    def test_upload_small(self):
        log = logging.getLogger('test_upload_small')
        log.info('start')
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        self.assertEqual(result, 'OK')

    def test_upload_small_and_listmatch(self):
        log = logging.getLogger('test_upload_small_and_listmatch')
        log.info('start')
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('listmatch')
        result = self._make_request(
            _base_url + '/data/test-key?action=listmatch')
        self.assertEqual(json.loads(result), [key])

    def test_upload_small_and_retrieve(self):
        log = logging.getLogger('test_upload_small_and_retrieve')
        log.info('start')
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('retrieve')
        result = self._make_request(
            _base_url + '/data/' + key)
        self.assertEqual(len(result), len(content))
        self.assertEqual(result, content)

    def test_upload_small_and_stat(self):
        log = logging.getLogger('test_upload_small_and_stat')
        log.info('start')
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        stat = {
            'timestamp': time.time(),
            'total_size': len(content),
            'userid': 0,
            'groupid': 0,
            'permissions': 0,
            'file_md5': hashlib.md5(content).hexdigest(),
            'file_adler32': zlib.adler32(content),
        }
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('retrieve')
        result = self._make_request(
            _base_url + '/data/' + key + '?action=stat')
        result = json.loads(result)
        expected_timestamp = stat.pop('timestamp')
        actual_timestamp = result.pop('timestamp')
        self.assertAlmostEqual(actual_timestamp, expected_timestamp, 0)
        self.assertEqual(result, stat)

    def test_retrieve_nonexistent_key(self):
        log = logging.getLogger('test_retrieve_nonexistent_key')
        log.info('start')
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/%s?action=delete' % (key,), '')
        log.info('retrieve')
        try:
            resp = self._make_request(
                _base_url + '/data/' + key, read_resp=False)
        except urllib2.HTTPError, err:
            self.assertEqual(err.code, 404)
        else:
            raise AssertionError('was expecting a 404 but got %d: %r' % (resp.code, resp.read()))

    def test_upload_large(self):
        log = logging.getLogger('test_upload_large')
        log.info('start')
        content = random_string(1024 * 1024 * 3)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        self.assertEqual(result, 'OK')

    def test_upload_large_and_retrieve(self):
        log = logging.getLogger('test_upload_large_and_retrieve')
        log.info('start')
        content = random_string(1024 * 1024 * 3)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('retrieve')
        result = self._make_request(
            _base_url + '/data/' + key)
        self.assertEqual(len(result), len(content))
        if result != content:
            diffs = filter(lambda (i, (r, c)): r != c, enumerate(zip(result, content)))
            raise AssertionError(
                'result differs from expected: '
                'start=%d, end=%d' % (
                    diffs[0][0],
                    diffs[-1][0]
                )
            )
        self.assertEqual(result, content)

    def test_upload_large_and_stat(self):
        log = logging.getLogger('test_upload_large_and_stat')
        log.info('start')
        content = random_string(1024 * 1024 * 3)
        key = self._key_generator.next()
        stat = {
            'timestamp': time.time(),
            'total_size': len(content),
            'userid': 0,
            'groupid': 0,
            'permissions': 0,
            'file_md5': hashlib.md5(content).hexdigest(),
            'file_adler32': zlib.adler32(content),
        }
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('retrieve')
        result = self._make_request(
            _base_url + '/data/' + key + '?action=stat')
        result = json.loads(result)
        expected_timestamp = stat.pop('timestamp')
        actual_timestamp = result.pop('timestamp')
        self.assertAlmostEqual(actual_timestamp, expected_timestamp, 0)
        self.assertEqual(result, stat)

    def test_upload_small_then_delete_and_listmatch(self):
        log = logging.getLogger('test_upload_small_then_delete_and_listmatch')
        log.info('start')
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('delete')
        result = self._make_request(
            _base_url + '/data/%s?action=delete' % (key,), '')
        log.info('listmatch')
        result = self._make_request(
            _base_url + '/data/test-key?action=listmatch')
        self.assertEqual(json.loads(result), [])

    def test_upload_small_then_delete_and_retrieve(self):
        log = logging.getLogger('test_upload_small_then_delete_and_retrieve')
        log.info('start')
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = self._make_request(
            _base_url + '/data/' + key, content)
        log.info('delete')
        result = self._make_request(
            _base_url + '/data/%s?action=delete' % (key,), '')
        log.info('retrieve')
        try:
            resp = self._make_request(
                _base_url + '/data/' + key, read_resp=False)
        except urllib2.HTTPError, err:
            self.assertEqual(err.code, 404)
        else:
            raise AssertionError('was expecting a 404 but got %d: %r' % (resp.code, resp.read()))


def _load_unit_tests(path):
    dotted_path = '.'.join(path.split('/'))
    _test_re = re.compile("test_.+?\.py$", re.IGNORECASE)
    _filename_to_module = lambda f: ('%s.%s' % (dotted_path,
                                                os.path.splitext(f)[0]))
    _load = unittest.defaultTestLoader.loadTestsFromModule
    files = os.listdir(path)
    files = filter(_test_re.search, files)
    module_names = map(_filename_to_module, files)
    for name in module_names:
        __import__(name)
    modules = [sys.modules[name] for name in module_names]
    return map(_load, modules)


if __name__ == "__main__":
    initialize_logging(_log_path)
    if 'prod' in sys.argv[1:]:
        _base_url = _prod_base_url
        tests = [unittest.defaultTestLoader.loadTestsFromTestCase(TestWebServer)]
    else:
        _base_url = _local_base_url
        tests = _load_unit_tests('unit_tests/web_server')
        if 'end-to-end' in sys.argv[1:]:
            tests.append(
                unittest.defaultTestLoader.loadTestsFromTestCase(TestWebServer))
    test_suite = unittest.TestSuite(tests)
    unittest.TextTestRunner(verbosity=2).run(test_suite)
