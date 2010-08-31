# -*- coding: utf-8 -*-
"""
test_amqp_database_server.py

test diyapi_web_server/amqp_database_server.py
"""
import unittest
import logging

from unit_tests.web_server import util

from messages.database_listmatch import DatabaseListMatch
from messages.database_listmatch_reply import DatabaseListMatchReply
from messages.space_usage import SpaceUsage
from messages.space_usage_reply import SpaceUsageReply
from messages.stat import Stat
from messages.stat_reply import StatReply

from diyapi_web_server.amqp_database_server import AMQPDatabaseServer

from diyapi_web_server.exceptions import (
    DatabaseServerDownError,
)


class TestAMQPDatabaseServer(unittest.TestCase):
    """test diyapi_web_server/amqp_database_server.py"""
    def setUp(self):
        self.log = logging.getLogger('TestAMQPDatabaseServer')
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange = 'test_exchange'
        self.server = AMQPDatabaseServer(self.amqp_handler, self.exchange)

    def test_listmatch(self):
        self.log.debug('test_listmatch')
        request_id = 'request_id'
        avatar_id = 1001
        prefix = 'prefix'
        key_list = ['key1', 'key2']
        message = DatabaseListMatch(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            prefix
        )
        reply = DatabaseListMatchReply(
            request_id,
            DatabaseListMatchReply.successful,
            key_list=key_list
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.server.listmatch(
            request_id,
            avatar_id,
            prefix
        )
        self.assertEqual(result, key_list)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_get_space_usage(self):
        self.log.debug('test_get_space_usage')
        request_id = 'request_id'
        avatar_id = 1001
        message = SpaceUsage(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name
        )
        reply = SpaceUsageReply(
            request_id,
            SpaceUsageReply.successful
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.server.get_space_usage(
            request_id,
            avatar_id
        )
        #self.assertEqual(result, key_list)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_stat(self):
        self.log.debug('test_stat')
        request_id = 'request_id'
        avatar_id = 1001
        path = 'some/path'
        stat = dict(
            timestamp=util.fake_time(),
            total_size=1235,
            file_adler32=-42,
            file_md5="ffff",
            userid=501,
            groupid=100,
            permissions=0o644,
        )
        message = Stat(
            request_id,
            avatar_id,
            path,
            0,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name
        )
        reply = StatReply(
            request_id,
            StatReply.successful,
            **stat
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.server.stat(
            request_id,
            avatar_id,
            path
        )
        self.assertEqual(result, stat)
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
