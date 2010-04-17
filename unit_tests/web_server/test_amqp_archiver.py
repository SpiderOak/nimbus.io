# -*- coding: utf-8 -*-
"""
test_amqp_archiver.py

test diyapi_web_server/amqp_archiver.py
"""
import os
import unittest
import uuid
import time
import hashlib
import zlib

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_final_reply import ArchiveKeyFinalReply

from diyapi_web_server.amqp_archiver import AMQPArchiver


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPArchiver(unittest.TestCase):
    """test diyapi_web_server/amqp_archiver.py"""
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

    def test_archive_entire(self):
        archiver = AMQPArchiver(self.handler, self.exchange_manager)
        avatar_id = 1001
        key = self._key_generator.next()
        timestamp = time.time()

        segments = []
        for segment_number in xrange(self.exchange_manager.num_exchanges):
            segment = random_string(64 * 1024)
            adler32 = zlib.adler32(segment)
            md5 = hashlib.md5(segment).digest()
            segments.append((segment, adler32, md5))
            request_id = uuid.UUID(int=segment_number).hex
            self.handler.replies_to_send[request_id] = [
                ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                )
            ]

        previous_size = archiver.archive_entire(avatar_id,
                                                key,
                                                [s[0] for s in segments],
                                                timestamp)

        self.assertEqual(previous_size, 0)
        self.assertEqual(len(self.channel.messages),
                         self.exchange_manager.num_exchanges)

        for segment_number, message in enumerate(self.channel.messages):
            ((amqp_message,), message_args) = message
            segment, adler32, md5 = segments[segment_number]
            self.assertEqual(message_args, dict(
                exchange=self.exchange_manager[segment_number][0],
                routing_key=ArchiveKeyEntire.routing_key,
                mandatory=True))
            message = ArchiveKeyEntire.unmarshall(amqp_message.body)
            self.assertEqual(message.avatar_id, avatar_id)
            self.assertEqual(message.key, key)
            self.assertEqual(message.timestamp, timestamp)
            self.assertEqual(message.segment_number, segment_number)
            self.assertEqual(message.adler32, adler32)
            self.assertEqual(message.md5, md5)
            self.assertEqual(message.content, segment)


if __name__ == "__main__":
    unittest.main()
