# -*- coding: utf-8 -*-
"""
test_amqp_retriever.py

test diyapi_web_server/amqp_retriever.py
"""
import os
import unittest

from unit_tests.web_server.test_amqp_archiver import (MockChannel,
                                                      FakeAMQPHandler)

from diyapi_web_server.amqp_retriever import AMQPRetriever

EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
MIN_EXCHANGES = len(EXCHANGES) - 2


class TestAMQPRetriever(unittest.TestCase):
    """test diyapi_web_server/amqp_retriever.py"""
    def setUp(self):
        self.channel = MockChannel()
        self.handler = FakeAMQPHandler()
        self.handler.channel = self.channel

    def test_retrieve(self):
        # TODO: make this test fail
        avatar_id = 1001
        retriever = AMQPRetriever(self.handler, EXCHANGES, MIN_EXCHANGES)


if __name__ == "__main__":
    unittest.main()
