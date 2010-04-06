# -*- coding: utf-8 -*-
"""
test_application.py

test diyapi_web_server/application.py
"""
import unittest
from webtest import TestApp

from unit_tests.util import random_string, generate_key
from unit_tests.web_server.test_amqp_archiver import (MockChannel,
                                                      FakeAMQPHandler)

from messages.archive_key_final_reply import ArchiveKeyFinalReply

from diyapi_web_server.application import Application


class TestApplication(unittest.TestCase):
    """test diyapi_web_server/application.py"""
    def setUp(self):
        self.channel = MockChannel()
        self.handler = FakeAMQPHandler()
        self.handler.channel = self.channel
        self.app = TestApp(Application(self.handler))
        self._key_generator = generate_key()

    def test_application(self):
        self.handler._reply_to_send = ArchiveKeyFinalReply(
            'request_id (replaced by FakeAMQPHandler)',
            ArchiveKeyFinalReply.successful,
            0)
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        resp = self.app.post('/' + key, content)
        resp.mustcontain('OK')



if __name__ == "__main__":
    unittest.main()
