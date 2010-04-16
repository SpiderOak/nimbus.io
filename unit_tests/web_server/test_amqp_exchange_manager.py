# -*- coding: utf-8 -*-
"""
test_amqp_exchange_manager.py

test diyapi_web_server/amqp_exchange_manager.py
"""
import os
import unittest

from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager, random


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
HANDOFF_NUM = 2


def fake_sample(population, k):
    """deterministic replacement for random.sample"""
    return list(population)[:k]

random.sample = fake_sample


class TestAMQPExchangeManager(unittest.TestCase):
    """test diyapi_web_server/amqp_exchange_manager.py"""
    def setUp(self):
        self.manager = AMQPExchangeManager(EXCHANGES, len(EXCHANGES) - 2)

    def test_initially_full(self):
        self.assertEqual(list(self.manager), EXCHANGES)

    def test_mark_down(self):
        self.manager.mark_down(3)
        self.assertEqual(len(self.manager), self.manager.num_exchanges - 1)
        expected = EXCHANGES[:3] + EXCHANGES[4:]
        self.assertEqual(list(self.manager), expected)

    def test_mark_up(self):
        self.manager.mark_down(3)
        self.manager.mark_up(3)
        self.assertEqual(len(self.manager), self.manager.num_exchanges)
        self.assertEqual(list(self.manager), EXCHANGES)

    def test_getitem_when_up(self):
        sequence_number = 3
        self.assertEqual(self.manager[sequence_number],
                         [EXCHANGES[sequence_number]])

    def test_getitem_when_down(self):
        sequence_number = 3
        self.manager.mark_down(sequence_number)
        expected = fake_sample(self.manager, HANDOFF_NUM)
        self.assertEqual(self.manager[sequence_number], expected)


if __name__ == "__main__":
    unittest.main()
