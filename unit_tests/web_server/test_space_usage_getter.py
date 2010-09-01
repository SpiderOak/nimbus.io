# -*- coding: utf-8 -*-
"""
test_space_usage_getter.py

test diyapi_web_server/space_usage_getter.py
"""
import logging
import os
import unittest
import uuid

from unit_tests.web_server import util

from diyapi_web_server.amqp_space_accounting_server import (
    AMQPSpaceAccountingServer)

from messages.space_usage import SpaceUsage
from messages.space_usage_reply import SpaceUsageReply

from diyapi_web_server.exceptions import (
    SpaceUsageFailedError,
)

from diyapi_web_server.space_usage_getter import SpaceUsageGetter


class TestSpaceUsageGetter(unittest.TestCase):
    """test diyapi_web_server/space_usage_getter.py"""
    def setUp(self):
        self.log = logging.getLogger('TestSpaceUsageGetter')
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange = 'test_exchange'
        self.accounting_server = AMQPSpaceAccountingServer(
            self.amqp_handler, self.exchange)
        self.getter = SpaceUsageGetter(self.accounting_server)
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_get_space_usage(self):
        self.log.debug('test_get_space_usage')
        avatar_id = 1001
        request_id = uuid.UUID(int=0).hex
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

        result = self.getter.get_space_usage(avatar_id, 0.1)

        self.assertEqual(result, usage)

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
