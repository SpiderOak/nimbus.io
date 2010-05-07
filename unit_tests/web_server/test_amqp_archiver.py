# -*- coding: utf-8 -*-
"""
test_amqp_archiver.py

test diyapi_web_server/amqp_archiver.py
"""
import os
import unittest
import uuid
import random
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
NUM_SEGMENTS = 10


class TestAMQPArchiver(unittest.TestCase):
    """test diyapi_web_server/amqp_archiver.py"""
    def setUp(self):
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next
        self._real_sample = random.sample
        random.sample = util.fake_sample

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1
        random.sample = self._real_sample

    def _make_small_data(self, avatar_id, timestamp, key):
        for segment_number in xrange(1, NUM_SEGMENTS + 1):
            segment = random_string(64 * 1024)
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            request_id = uuid.UUID(int=segment_number - 1).hex
            message = ArchiveKeyEntire(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                key,
                0, # version number
                segment_number,
                -42,
                'ffffffff',
                segment_adler32,
                segment_md5,
                segment
            )
            reply = ArchiveKeyFinalReply(
                request_id,
                ArchiveKeyFinalReply.successful,
                0
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, exchange
                )] = [reply]
            yield message

    def test_archive_small(self):
        avatar_id = 1001
        timestamp = time.time()
        key = self._key_generator.next()
        messages = list(self._make_small_data(
            avatar_id, timestamp, key))

        archiver = AMQPArchiver(
            self.amqp_handler,
            self.exchange_manager,
            avatar_id,
            key,
            timestamp
        )
        previous_size = archiver.archive_final(
            messages[0].file_adler32,
            messages[0].file_md5,
            [message.content for message in messages]
        )

        self.assertEqual(previous_size, 0)

        expected = [(
            message.marshall(),
            self.exchange_manager[message.segment_number - 1][0]
        ) for message in messages]
        actual = [(
            message.marshall(),
            exchange
        ) for message, exchange in self.amqp_handler.messages]
        self.assertEqual(actual, expected)

    def test_archive_small_with_handoff(self):
        avatar_id = 1001
        timestamp = time.time()
        key = self._key_generator.next()
        self.exchange_manager.mark_down(0)
        messages = list(self._make_small_data(
            avatar_id, timestamp, key))
        self.exchange_manager.mark_up(0)

        archiver = AMQPArchiver(
            self.amqp_handler,
            self.exchange_manager,
            avatar_id,
            key,
            timestamp
        )

        expected = [(
            message.marshall(),
            self.exchange_manager[message.segment_number - 1][0]
        ) for message in messages]

        previous_size = archiver.archive_final(
            messages[0].file_adler32,
            messages[0].file_md5,
            [message.content for message in messages],
            0
        )

        self.assertEqual(previous_size, 0)

        self.assertTrue(self.exchange_manager.is_down(0))
        expected += [(
            messages[0].marshall(),
            exchange
        ) for exchange in self.exchange_manager[0]]
        self.exchange_manager.mark_up(0)
        actual = [(
            message.marshall(),
            exchange
        ) for message, exchange in self.amqp_handler.messages]
        self.assertEqual(actual, expected)


if __name__ == "__main__":
    unittest.main()
