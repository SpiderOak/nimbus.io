# -*- coding: utf-8 -*-
"""
test_amqp_handoff_sender.py

test diyapi_web_server/amqp_handoff_sender.py
"""
import os
import random
import unittest

from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager

from diyapi_web_server.amqp_handoff_sender import AMQPHandoffSender


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPHandoffSender(unittest.TestCase):
    """test diyapi_web_server/amqp_handoff_sender.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self.handler = util.FakeAMQPHandler()
        self.sender = AMQPHandoffSender(self.handler, self.exchange_manager, 0)
        self._real_sample = random.sample
        random.sample = util.fake_sample

    def tearDown(self):
        random.sample = self._real_sample

    def test_send_to_exchange_when_up(self):
        message = util.FakeMessage('routing key', 'body', 'request id')
        self.handler.replies_to_send['request id'] = [message]
        reply_queue = self.sender.send_to_exchange(0, message)
        self.assertEqual(
            [(message, exchange) for exchange in self.exchange_manager[0]],
            self.handler.messages
        )
        reply = reply_queue.get(0.1)
        self.assertEqual(reply, message)

    def test_handoff_when_down(self):
        self.exchange_manager.mark_down(0)
        self.test_send_to_exchange_when_up()

    def test_handoff_when_no_response(self):
        message = util.FakeMessage('routing key', 'body', 'request id')
        self.exchange_manager.mark_down(0)
        for exchange in self.exchange_manager[0]:
            self.handler.replies_to_send_by_exchange[
                'request id', exchange] = [message]
        self.exchange_manager.mark_up(0)
        messages = [(message, exchange)
                    for exchange in self.exchange_manager[0]]
        reply_queue = self.sender.send_to_exchange(0, message)
        for i in xrange(self.exchange_manager.num_exchanges):
            self.assertEqual(self.exchange_manager.is_down(i), i == 0)
        messages.extend([(message, exchange)
                         for exchange in self.exchange_manager[0]])
        self.assertEqual(messages, self.handler.messages)
        reply = reply_queue.get(0.1)
        self.assertEqual(reply, message)


if __name__ == "__main__":
    unittest.main()
