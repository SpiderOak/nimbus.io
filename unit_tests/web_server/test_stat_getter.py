# -*- coding: utf-8 -*-
"""
test_stat_getter.py

test diyapi_web_server/stat_getter.py
"""
import os
import unittest
import uuid

from unit_tests.web_server import util

from diyapi_web_server.amqp_database_server import AMQPDatabaseServer

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
        self.database_servers = [
            AMQPDatabaseServer(self.amqp_handler, exchange)
            for exchange in EXCHANGES
        ]
        self.getter = StatGetter(self.database_servers, AGREEMENT_LEVEL)
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_stat(self):
        avatar_id = 1001
        path = 'some/path'
        stat = dict(
            timestamp=util.fake_time(),
            total_size=1235,
            file_adler=-42,
            file_md5="ffff",
            userid=501,
            groupid=100,
            permissions=0o644,
        )
        messages = []
        for i, server in enumerate(self.database_servers):
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
                StatReply.successful,
                **stat
            )
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, server.exchange
            )].put(reply)
            messages.append((message, server.exchange))

        result = self.getter.stat(avatar_id, path, 0)

        self.assertEqual(result, stat)

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

    def test_stat_fails_with_disagreement(self):
        avatar_id = 1001
        path = 'some/path'
        stat1 = dict(
            timestamp=util.fake_time(),
            total_size=1235,
            file_adler=-42,
            file_md5="ffff",
            userid=501,
            groupid=100,
            permissions=0o644,
        )
        stat2 = dict(
            timestamp=util.fake_time() + 5,
            total_size=1235,
            file_adler=-42,
            file_md5="ffff",
            userid=501,
            groupid=100,
            permissions=0o644,
        )
        messages = []
        for i, server in enumerate(self.database_servers):
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
                StatReply.successful,
                **(stat2 if i < AGREEMENT_LEVEL - 1 else stat1)
            )
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, server.exchange
            )].put(reply)
            messages.append((message, server.exchange))

        self.assertRaises(
            StatFailedError,
            self.getter.stat,
            avatar_id,
            path,
            0
        )

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
