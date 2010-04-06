# -*- coding: utf-8 -*-
"""
test_web_server.py

test diyapi_web_server/diyapi_web_server_main.py
"""
# importing this monkey-patches socket, putting us in geventland
from diyapi_web_server import WebServer

import os
import re
import sys
import shutil
import logging
import unittest
import urllib2

from diyapi_tools.standard_logging import initialize_logging
from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_web_server.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path


class TestWebServer(unittest.TestCase):
    """test diyapi_web_server/diyapi_web_server_main.py"""

    def setUp(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()

        self.server = WebServer()
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def test_upload_small(self):
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = urllib2.urlopen('http://127.0.0.1:8088/archive/' + key,
                                 content).read()
        self.assertEqual(result, 'OK')

    def test_upload_small_and_listmatch(self):
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        result = urllib2.urlopen('http://127.0.0.1:8088/archive/' + key,
                                 content).read()
        result = urllib2.urlopen('http://127.0.0.1:8088/listmatch?prefix=test-key').read()
        self.assertEqual(result, repr([key]))


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
    tests = _load_unit_tests('unit_tests/web_server')
    if 'end-to-end' in sys.argv[1:]:
        tests.append(
            unittest.defaultTestLoader.loadTestsFromTestCase(TestWebServer))
    test_suite = unittest.TestSuite(tests)
    unittest.TextTestRunner(verbosity=2).run(test_suite)
