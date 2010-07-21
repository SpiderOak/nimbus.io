# -*- coding: utf-8 -*-
"""
test_listmatcher.py

test diyapi_web_server/listmatcher.py
"""
import os
import unittest
import uuid

from unit_tests.web_server import util

from diyapi_web_server.amqp_data_reader import AMQPDataReader

from messages.database_listmatch import DatabaseListMatch
from messages.database_listmatch_reply import DatabaseListMatchReply

from diyapi_web_server.exceptions import (
    ListmatchFailedError,
)

from diyapi_web_server.listmatcher import Listmatcher


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
AGREEMENT_LEVEL = 8


class TestListmatcher(unittest.TestCase):
    """test diyapi_web_server/listmatcher.py"""
    def setUp(self):
        self.amqp_handler = util.FakeAMQPHandler()
        self.data_readers = [AMQPDataReader(self.amqp_handler, exchange)
                             for exchange in EXCHANGES]
        self.listmatcher = Listmatcher(self.data_readers, AGREEMENT_LEVEL)
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_listmatch(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        messages = []
        for i, data_reader in enumerate(self.data_readers):
            request_id = uuid.UUID(int=i).hex
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
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, data_reader.exchange
            )].put(reply)
            messages.append((message, data_reader.exchange))

        result = self.listmatcher.listmatch(avatar_id, prefix, 0)

        self.assertEqual(result, key_list)

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

    def test_listmatch_with_down_nodes(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        for i, data_reader in enumerate(self.data_readers[:AGREEMENT_LEVEL]):
            request_id = uuid.UUID(int=i).hex
            reply = DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=key_list
            )
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, data_reader.exchange
            )].put(reply)
        result = self.listmatcher.listmatch(avatar_id, prefix, 0)
        self.assertEqual(result, key_list)

    def test_listmatch_with_not_enough_nodes(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        for i, data_reader in enumerate(
            self.data_readers[:AGREEMENT_LEVEL - 1]):
                request_id = uuid.UUID(int=i).hex
                reply = DatabaseListMatchReply(
                    request_id,
                    DatabaseListMatchReply.successful,
                    key_list=key_list
                )
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_reader.exchange
                )].put(reply)
        self.assertRaises(
            ListmatchFailedError,
            self.listmatcher.listmatch,
            avatar_id,
            prefix,
            0
        )

    def test_listmatch_excludes_disagreeing_items(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list1 = ['%s-%d' % (prefix, i) for i in xrange(5)]
        key_list2 = ['%s-%d' % (prefix, i) for i in xrange(10)]
        for i, data_reader in enumerate(self.data_readers):
            request_id = uuid.UUID(int=i).hex
            reply = DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=(key_list2 if i < AGREEMENT_LEVEL - 1 else key_list1)
            )
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, data_reader.exchange
            )].put(reply)
        result = self.listmatcher.listmatch(avatar_id, prefix, 0)
        self.assertEqual(result, key_list1)


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
