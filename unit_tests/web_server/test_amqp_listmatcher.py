# -*- coding: utf-8 -*-
"""
test_amqp_listmatcher.py

test diyapi_web_server/amqp_listmatcher.py
"""
import os
import unittest

from unit_tests.web_server.util import MockChannel, FakeAMQPHandler
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.database_listmatch_reply import DatabaseListMatchReply

from diyapi_web_server.amqp_listmatcher import AMQPListmatcher


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPListmatcher(unittest.TestCase):
    """test diyapi_web_server/amqp_listmatcher.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(
            EXCHANGES, len(EXCHANGES) - 2)
        self.channel = MockChannel()
        self.handler = FakeAMQPHandler()
        self.handler.channel = self.channel

    def test_listmatch(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        self.handler._reply_to_send = DatabaseListMatchReply(
            'request_id (replaced by FakeAMQPHandler)',
            DatabaseListMatchReply.successful,
            key_list=key_list)

        matcher = AMQPListmatcher(self.handler, self.exchange_manager)
        keys = matcher.listmatch(avatar_id, prefix, 0.1)
        self.assertEqual(keys, key_list)


if __name__ == "__main__":
    unittest.main()
