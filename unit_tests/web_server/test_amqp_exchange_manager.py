# -*- coding: utf-8 -*-
"""
test_amqp_exchange_manager.py

test diyapi_web_server/amqp_exchange_manager.py
"""
import os
import random
import unittest

from unit_tests.web_server import util

from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
HANDOFF_NUM = 2


class TestAMQPExchangeManager(unittest.TestCase):
    """test diyapi_web_server/amqp_exchange_manager.py"""
    def setUp(self):
        self.manager = AMQPExchangeManager(EXCHANGES)
        self._real_sample = random.sample
        random.sample = util.fake_sample

    def tearDown(self):
        random.sample = self._real_sample

    def test_initially_full(self):
        self.assertEqual(list(self.manager), EXCHANGES)
        self.assertEqual(self.manager.num_exchanges, len(EXCHANGES))

    def test_mark_down(self):
        self.manager.mark_down(3)
        self.assertEqual(len(self.manager), len(EXCHANGES) - 1)
        expected = EXCHANGES[:3] + EXCHANGES[4:]
        self.assertEqual(list(self.manager), expected)

    def test_mark_up(self):
        self.manager.mark_down(3)
        self.manager.mark_up(3)
        self.assertEqual(len(self.manager), len(EXCHANGES))
        self.assertEqual(list(self.manager), EXCHANGES)

    def test_getitem_when_up(self):
        exchange_num = 3
        self.assertEqual(self.manager[exchange_num],
                         [EXCHANGES[exchange_num]])

    def test_getitem_when_down(self):
        exchange_num = 3
        self.manager.mark_down(exchange_num)
        expected = util.fake_sample(self.manager, HANDOFF_NUM)
        self.assertEqual(self.manager[exchange_num], expected)

    def test_is_down(self):
        for i in xrange(self.manager.num_exchanges):
            self.assertFalse(self.manager.is_down(i))
        self.manager.mark_down(2)
        self.assertTrue(self.manager.is_down(2))
        self.assertFalse(self.manager.is_down(1))


if __name__ == "__main__":
    unittest.main()
