# -*- coding: utf-8 -*-
"""
test_amqp_space_accounter.py

test diyapi_web_server/amqp_space_accounter.py
"""
import unittest

from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.space_accounting_detail import SpaceAccountingDetail

from diyapi_web_server.amqp_space_accounter import AMQPSpaceAccounter


class TestAMQPSpaceAccounter(unittest.TestCase):
    """test diyapi_web_server/amqp_space_accounter.py"""
    def setUp(self):
        self.exchange = 'space-accounting-exchange'
        self.channel = util.MockChannel()
        self.amqp_handler = util.FakeAMQPHandler()
        self.amqp_handler.channel = self.channel
        self.accounter = AMQPSpaceAccounter(self.amqp_handler, self.exchange)

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
            actual, expected, 'accounter did not send expected messages')

    def test_added(self):
        avatar_id = 1001
        timestamp = 123456789.1011
        bytes = 1024 * 1024
        self.accounter.added(avatar_id, timestamp, bytes)
        self._check_messages(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_added,
            bytes
        )

    def test_retrieved(self):
        avatar_id = 1001
        timestamp = 123456789.1011
        bytes = 1024 * 1024
        self.accounter.retrieved(avatar_id, timestamp, bytes)
        self._check_messages(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_retrieved,
            bytes
        )

    def test_removed(self):
        avatar_id = 1001
        timestamp = 123456789.1011
        bytes = 1024 * 1024
        self.accounter.removed(avatar_id, timestamp, bytes)
        self._check_messages(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_removed,
            bytes
        )
