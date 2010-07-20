# -*- coding: utf-8 -*-
"""
test_process.py

test diyapi_web_server/amqp_process.py
"""
import unittest
import logging

from unit_tests.web_server import util

from diyapi_web_server.amqp_process import AMQPProcess


class DownError(Exception):
    pass

class SomeError(Exception):
    pass

class ProcessWithDownError(AMQPProcess):
    _downerror_class = DownError


class TestAMQPProcess(unittest.TestCase):
    """test diyapi_web_server/amqp_process.py"""
    def setUp(self):
        self.log = logging.getLogger('TestAMQPProcess')
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange = 'test_exchange'
        self.process = AMQPProcess(self.amqp_handler, self.exchange)

    def test_hash(self):
        self.log.debug('test_hash')
        self.assertEquals(hash(self.process), hash(self.exchange))

    def test_eq(self):
        self.log.debug('test_eq')
        self.assertEquals(
            self.process, AMQPProcess(self.amqp_handler, self.exchange))
        self.assertNotEquals(
            self.process, AMQPProcess(self.amqp_handler, 'other-exchange'))
        self.assertNotEquals(self.process, 5)

    def test_mark_down(self):
        self.log.debug('test_mark_down')
        self.assertFalse(self.process.is_down)
        self.process.mark_down()
        self.assertTrue(self.process.is_down)

    def test_mark_up(self):
        self.log.debug('test_mark_up')
        self.process.mark_down()
        self.assertTrue(self.process.is_down)
        self.process.mark_up()
        self.assertFalse(self.process.is_down)

    def test_send_when_down_raises_downerror(self):
        self.log.debug('test_send_when_down_raises_downerror')
        process = ProcessWithDownError(self.amqp_handler, self.exchange)
        process.mark_down()
        self.assertRaises(DownError, process._send, None, None)

    def test_send(self):
        self.log.debug('test_send')
        request_id = 'some-request-id'
        reply = util.FakeMessage(
            'some-routing-key', 'reply body', request_id)
        self.amqp_handler.replies_to_send[request_id].put(reply)
        message = util.FakeMessage(
            'some-routing-key', 'message body', request_id)
        result = self.process._send(message, SomeError)

        self.assertEqual(reply.marshall(), result.marshall())

        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_send_with_reply_error(self):
        self.log.debug('test_send_with_reply_error')
        request_id = 'some-request-id'
        reply = util.FakeMessage(
            'some-routing-key', 'reply body', request_id)
        reply.error = True
        reply.result = 1
        reply.error_message = 'error message'
        self.amqp_handler.replies_to_send[request_id].put(reply)
        message = util.FakeMessage(
            'some-routing-key', 'message body', request_id)

        self.assertRaises(
            SomeError,
            self.process._send,
            message,
            SomeError
        )

        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
