# -*- coding: utf-8 -*-
"""
test_amqp_handler.py

test diyapi_web_server/amqp_handler.py
"""
import unittest
from cStringIO import StringIO

from unit_tests.util import random_string
from unit_tests.web_server.util import MockChannel, FakeMessage

from diyapi_tools import amqp_connection
from diyapi_web_server.amqp_handler import AMQPHandler


class TestAMQPHandler(unittest.TestCase):
    """test diyapi_web_server/amqp_handler.py"""

    def setUp(self):
        self.channel = MockChannel()
        self.handler = AMQPHandler()
        self.handler.channel = self.channel

    def test_send_message(self):
        message = FakeMessage('some.routing.key', 'hello world')
        self.handler.send_message(message)
        ((amqp_message,), message_args) = self.channel.messages[0]
        self.assertEqual(message_args, dict(
            exchange=self.handler.exchange,
            routing_key=message.routing_key,
            mandatory=True))
        self.assertEqual(amqp_message.body, message.body)

    def test_send_message_and_receive_reply(self):
        message = FakeMessage('some.routing.key',
                              'hello world',
                              'my_request_id')
        replies = self.handler.send_message(message)
        self.assertTrue(replies.empty())
        # TODO: call handler._callback with a fake reply


if __name__ == "__main__":
    unittest.main()
