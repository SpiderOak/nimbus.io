# -*- coding: utf-8 -*-
"""
test_stat_getter.py

test diyapi_web_server/stat_getter.py
"""
import os
import unittest
import uuid

from unit_tests.web_server import util

from diyapi_web_server.amqp_data_reader import AMQPDataReader

from messages.stat import Stat
from messages.stat_reply import StatReply

from diyapi_web_server.exceptions import (
    StatFailedError,
)

from diyapi_web_server.stat_getter import StatGetter


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
AGREEMENT_LEVEL = 8


class TestStatGetter(unittest.TestCase):
    """test diyapi_web_server/stat_getter.py"""
    def setUp(self):
        self.amqp_handler = util.FakeAMQPHandler()
        self.data_readers = [AMQPDataReader(self.amqp_handler, exchange)
                             for exchange in EXCHANGES]
        self.getter = StatGetter(self.data_readers, AGREEMENT_LEVEL)
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_stat(self):
        avatar_id = 1001
        path = 'some/path'
        messages = []
        for i, data_reader in enumerate(self.data_readers):
            request_id = uuid.UUID(int=i).hex
            message = Stat(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                path
            )
            reply = StatReply(
                request_id,
                StatReply.successful
            )
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, data_reader.exchange
            )].put(reply)
            messages.append((message, data_reader.exchange))

        result = self.getter.stat(avatar_id, path, 0)

        self.assertEqual(result, None)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected)


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
