# -*- coding: utf-8 -*-
"""
test_amqp_destroyer.py

test diyapi_web_server/amqp_destroyer.py
"""
import os
import unittest
import uuid
import time

from unit_tests.util import generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply

from diyapi_web_server.amqp_destroyer import AMQPDestroyer


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPDestroyer(unittest.TestCase):
    """test diyapi_web_server/amqp_destroyer.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self.channel = util.MockChannel()
        self.handler = util.FakeAMQPHandler()
        self.handler.channel = self.channel
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_destroy(self):
        avatar_id = 1001
        key = self._key_generator.next()
        base_size = 12345
        timestamp = time.time()
        num_segments = self.exchange_manager.num_exchanges
        for segment_number in xrange(1, num_segments - 1):
            request_id = uuid.UUID(int=segment_number - 1).hex
            self.handler.replies_to_send[request_id] = [
                DatabaseKeyDestroyReply(
                    request_id,
                    DatabaseKeyDestroyReply.successful,
                    base_size + segment_number - 1
                )
            ]

        destroyer = AMQPDestroyer(self.handler, self.exchange_manager)
        size_deleted = destroyer.destroy(avatar_id, key, timestamp, 0.1)
        self.assertEqual(size_deleted, base_size)


if __name__ == "__main__":
    unittest.main()
