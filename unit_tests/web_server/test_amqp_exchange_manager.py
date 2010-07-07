# -*- coding: utf-8 -*-
"""
test_amqp_exchange_manager.py

test diyapi_web_server/amqp_exchange_manager.py
"""
import os
import unittest

from unit_tests.web_server import util

from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPExchangeManager(unittest.TestCase):
    """test diyapi_web_server/amqp_exchange_manager.py"""
    def setUp(self):
        self.manager = AMQPExchangeManager(EXCHANGES)

    def test_initially_full(self):
        self.assertEqual(list(self.manager), EXCHANGES)
        for i in xrange(len(self.manager)):
            self.assertFalse(self.manager.is_down(i), i)

    def test_mark_down(self):
        self.manager.mark_down(3)
        self.assertTrue(self.manager.is_down(3))
        for i in xrange(len(self.manager)):
            if i == 3:
                continue
            self.assertFalse(self.manager.is_down(i), i)

    def test_mark_up(self):
        self.manager.mark_down(3)
        self.manager.mark_up(3)
        for i in xrange(len(self.manager)):
            self.assertFalse(self.manager.is_down(i), i)

    def test_up(self):
        self.manager.mark_down(3)
        self.assertEqual(self.manager.up(),
                         self.manager[:3] + self.manager[4:])


if __name__ == "__main__":
    unittest.main()
