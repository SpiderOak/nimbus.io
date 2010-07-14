# -*- coding: utf-8 -*-
"""
test_amqp_handler.py

test diyapi_web_server/amqp_handler.py
"""
import unittest
from cStringIO import StringIO

from unit_tests.util import random_string
from unit_tests.web_server.util import (
    MockChannel,
    FakeMessage,
    FakeAMQPMessage,
)

from diyapi_tools import amqp_connection
from diyapi_web_server.amqp_handler import AMQPHandler, MESSAGE_TYPES


class TestAMQPHandler(unittest.TestCase):
    """test diyapi_web_server/amqp_handler.py"""

    def setUp(self):
        self.channel = MockChannel()
        self.handler = AMQPHandler()
        self.handler.channel = self.channel
        self.routing_key = 'some.routing.key'
        MESSAGE_TYPES[self.routing_key] = FakeMessage

    def tearDown(self):
        del MESSAGE_TYPES[self.routing_key]

    def test_send_message(self):
        message = FakeMessage(self.routing_key, 'hello world')
        self.handler.send_message(message)
        ((amqp_message,), message_args) = self.channel.messages[0]
        self.assertEqual(message_args, dict(
            exchange=self.handler.exchange,
            routing_key=message.routing_key,
            mandatory=True))
        self.assertEqual(amqp_message.body, message.marshall())

    def test_send_message_and_receive_reply(self):
        request_id = 'my_request_id'
        message = FakeMessage(self.routing_key, 'hello world', request_id)
        replies = self.handler.send_message(message)

        self.assertTrue(replies.empty())

        reply = FakeMessage(self.routing_key, 'reply body', request_id)
        amqp_reply = FakeAMQPMessage(self.routing_key, reply.marshall())
        self.handler._callback(amqp_reply)

        self.assertEqual(replies.get().marshall(), reply.marshall())

    def test_subscribe(self):
        messages = []
        def callback(message):
            messages.append(message)
        self.handler.subscribe(FakeMessage, callback)

        message = FakeMessage(self.routing_key, 'test message')
        amqp_message = FakeAMQPMessage(self.routing_key, message.marshall())
        self.handler._callback(amqp_message)

        self.assertEqual(
            [m.marshall() for m in messages],
            [message.marshall()]
        )


if __name__ == "__main__":
    unittest.main()
