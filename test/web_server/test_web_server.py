# -*- coding: utf-8 -*-
"""
test_web_server.py

test diyapi_web_server/diyapi_web_server_main.py
"""
# importing this monkey-patches socket, putting us in geventland
from diyapi_web_server import WebServer

import os
import shutil
import logging
import unittest
import urllib2

from tools.standard_logging import initialize_logging
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
        result = urllib2.urlopen('http://127.0.0.1:8088/' + key,
                                 content).read()
        self.assertEqual(result, 'POST /%s (%d)' % (key, len(content)))


if __name__ == "__main__":
    unittest.main()
