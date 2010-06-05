# -*- coding: utf-8 -*-
"""
test_amqp_listmatcher.py

test diyapi_web_server/amqp_listmatcher.py
"""
import os
import unittest
import uuid
import random

from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.database_listmatch_reply import DatabaseListMatchReply

from diyapi_web_server.amqp_listmatcher import AMQPListmatcher


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPListmatcher(unittest.TestCase):
    """test diyapi_web_server/amqp_listmatcher.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self.channel = util.MockChannel()
        self.handler = util.FakeAMQPHandler()
        self.handler.channel = self.channel
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next
        self._real_choice = random.choice
        random.choice = util.fake_choice

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1
        random.choice = self._real_choice

    def test_listmatch(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        request_id = uuid.UUID(int=0).hex
        self.handler.replies_to_send_by_exchange[(
            request_id,
            random.choice(self.exchange_manager)
        )].put(
            DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=key_list
            )
        )

        matcher = AMQPListmatcher(self.handler, self.exchange_manager)
        keys = matcher.listmatch(avatar_id, prefix, 0.1)
        self.assertEqual(keys, key_list)


if __name__ == "__main__":
    unittest.main()
