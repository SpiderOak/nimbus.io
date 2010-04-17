# -*- coding: utf-8 -*-
"""
test_application.py

test diyapi_web_server/application.py
"""
import os
import unittest
import uuid
from webtest import TestApp

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_listmatch_reply import DatabaseListMatchReply

from diyapi_web_server.application import Application


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestApplication(unittest.TestCase):
    """test diyapi_web_server/application.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(
            EXCHANGES, len(EXCHANGES) - 2)
        self.channel = util.MockChannel()
        self.handler = util.FakeAMQPHandler()
        self.handler.channel = self.channel
        self.app = TestApp(Application(self.handler, self.exchange_manager))
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_archive(self):
        for i in xrange(self.exchange_manager.num_exchanges):
            request_id = uuid.UUID(int=i).hex
            self.handler.replies_to_send[request_id] = [
                ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                )
            ]
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        resp = self.app.post('/data/' + key, content)
        self.assertEqual(resp.body, 'OK')

    def test_listmatch(self):
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        request_id = uuid.UUID(int=0).hex
        self.handler.replies_to_send[request_id] = [
            DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=key_list
            )
        ]
        resp = self.app.get('/data/%s' % (prefix,), dict(action='listmatch'))
        self.assertEqual(resp.body, repr(key_list))


if __name__ == "__main__":
    unittest.main()
