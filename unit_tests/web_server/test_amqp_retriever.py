# -*- coding: utf-8 -*-
"""
test_amqp_retriever.py

test diyapi_web_server/amqp_retriever.py
"""
import os
import unittest
import uuid
import time

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager

from messages.retrieve_key_start_reply import RetrieveKeyStartReply

from diyapi_web_server.amqp_retriever import AMQPRetriever


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPRetriever(unittest.TestCase):
    """test diyapi_web_server/amqp_retriever.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(
            EXCHANGES, len(EXCHANGES) - 2)
        self.channel = util.MockChannel()
        self.handler = util.FakeAMQPHandler()
        self.handler.channel = self.channel
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_retrieve(self):
        avatar_id = 1001
        key = self._key_generator.next()
        timestamp = time.time()

        segments = []
        for segment_number in xrange(self.exchange_manager.num_exchanges):
            segment = random_string(64 * 1024)
            request_id = uuid.UUID(int=segment_number).hex
            self.handler.replies_to_send[request_id] = [
                RetrieveKeyStartReply(
                    request_id,
                    RetrieveKeyStartReply.successful,
                    timestamp,
                    False,
                    0,
                    segment_number,
                    self.exchange_manager.num_exchanges,
                    12345,
                    123450,
                    -42,
                    'ffffffffff',
                    32,
                    '1111111111111111',
                    segment
                )
            ]
            segments.append(segment)

        retriever = AMQPRetriever(self.handler, self.exchange_manager)
        self.assertEqual(retriever.retrieve(avatar_id, key, 0.1),
                         dict(enumerate(segments)))


if __name__ == "__main__":
    unittest.main()
