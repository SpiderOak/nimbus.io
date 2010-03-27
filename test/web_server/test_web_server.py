# -*- coding: utf-8 -*-
"""
test_web_server.py

test diyapi_web_server/diyapi_web_server_main.py
"""
# importing this monkey-patches socket, putting us in geventland
from diyapi_web_server.diyapi_web_server_main import make_wsgi_server

import logging
import unittest
import urllib2


class TestWebServer(unittest.TestCase):
    """test diyapi_web_server/diyapi_web_server_main.py"""

    def setUp(self):
        self.server = make_wsgi_server()
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def test_root(self):
        data = urllib2.urlopen('http://127.0.0.1:8088/').read()
        self.assertEqual(data, 'hello world')


if __name__ == "__main__":
    unittest.main()
