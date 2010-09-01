# -*- coding: utf-8 -*-
"""
test_amqp_space_accounting_server.py

test diyapi_web_server/amqp_space_accounting_server.py
"""
import unittest
import logging

from unit_tests.web_server import util

from messages.space_accounting_detail import SpaceAccountingDetail
from messages.space_usage import SpaceUsage
from messages.space_usage_reply import SpaceUsageReply

from diyapi_web_server.amqp_space_accounting_server import (
    AMQPSpaceAccountingServer)

from diyapi_web_server.exceptions import (
    SpaceAccountingServerDownError,
)


class TestAMQPSpaceAccountingServer(unittest.TestCase):
    """test diyapi_web_server/amqp_space_accounting_server.py"""
    def setUp(self):
        self.log = logging.getLogger('TestAMQPSpaceAccountingServer')
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange = 'test_exchange'
        self.server = AMQPSpaceAccountingServer(
            self.amqp_handler, self.exchange)

    def _check_messages(self, avatar_id, timestamp, event, value):
        expected = [
            (
                SpaceAccountingDetail(
                    avatar_id,
                    timestamp,
                    event,
                    value
                ).marshall(),
                self.exchange
            ),
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'server did not send expected messages')

    def test_added(self):
        self.log.debug('test_added')
        avatar_id = 1001
        timestamp = 123456789.1011
        bytes = 1024 * 1024
        self.server.added(avatar_id, timestamp, bytes)
        self._check_messages(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_added,
            bytes
        )

    def test_retrieved(self):
        self.log.debug('test_retrieved')
        avatar_id = 1001
        timestamp = 123456789.1011
        bytes = 1024 * 1024
        self.server.retrieved(avatar_id, timestamp, bytes)
        self._check_messages(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_retrieved,
            bytes
        )

    def test_removed(self):
        self.log.debug('test_removed')
        avatar_id = 1001
        timestamp = 123456789.1011
        bytes = 1024 * 1024
        self.server.removed(avatar_id, timestamp, bytes)
        self._check_messages(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_removed,
            bytes
        )

    def test_get_space_usage(self):
        self.log.debug('test_get_space_usage')
        request_id = 'request_id'
        avatar_id = 1001
        usage = {
            'bytes_added': 234,
            'bytes_removed': 6324,
            'bytes_retrieved': 98108,
        }
        message = SpaceUsage(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name
        )
        reply = SpaceUsageReply(
            request_id,
            SpaceUsageReply.successful,
            **usage
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.server.get_space_usage(
            request_id,
            avatar_id
        )
        self.assertEqual(result, usage)
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
