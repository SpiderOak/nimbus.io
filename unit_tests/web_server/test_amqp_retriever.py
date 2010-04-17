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

from messages.database_key_list_reply import DatabaseKeyListReply
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from diyapi_database_server.database_content import factory as content_factory

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
        # TODO: make this test fail
        avatar_id = 1001
        key = self._key_generator.next()
        timestamp = time.time()

        segments = []
        for segment_number in xrange(self.exchange_manager.num_exchanges):
            segment = random_string(64 * 1024)
            database_content = content_factory(
                False,
                timestamp,
                0,
                segment_number,
                self.exchange_manager.num_exchanges,
                12345,
                123450,
                -42,
                'ffffffffff',
                key
            )
            segments.append((segment, database_content))
            request_id = uuid.UUID(int=segment_number).hex
            self.handler.replies_to_send[request_id] = [
                DatabaseKeyListReply(
                    request_id,
                    DatabaseKeyListReply.successful,
                    [database_content]
                )
            ]

        for i, (segment, database_content) in enumerate(segments):
            request_id = uuid.UUID(int=segment_number + i + 1).hex
            self.handler.replies_to_send[request_id] = [
                RetrieveKeyStartReply(
                    request_id,
                    RetrieveKeyStartReply.successful,
                    database_content.timestamp,
                    database_content.is_tombstone,
                    database_content.version_number,
                    database_content.segment_number,
                    database_content.segment_count,
                    database_content.segment_size,
                    database_content.total_size,
                    database_content.adler32,
                    database_content.md5,
                    segment
                )
            ]

        retriever = AMQPRetriever(self.handler, self.exchange_manager)
        self.assertEqual(retriever.retrieve(avatar_id, key, 0.1),
                         [segment for segment, database_content in segments])


if __name__ == "__main__":
    unittest.main()
