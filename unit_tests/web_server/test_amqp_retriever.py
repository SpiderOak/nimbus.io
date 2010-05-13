# -*- coding: utf-8 -*-
"""
test_amqp_retriever.py

test diyapi_web_server/amqp_retriever.py
"""
import os
import unittest
import uuid
import time
import zlib
import hashlib

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply

from diyapi_web_server.amqp_retriever import AMQPRetriever


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
NUM_SEGMENTS = 10


class TestAMQPRetriever(unittest.TestCase):
    """test diyapi_web_server/amqp_retriever.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self.channel = util.MockChannel()
        self.amqp_handler = util.FakeAMQPHandler()
        self.amqp_handler.channel = self.channel
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def _make_small_data(self, avatar_id, timestamp, key):
        file_size = NUM_SEGMENTS
        file_adler32 = -42
        file_md5 = 'ffffff'
        messages = []
        segments = []
        for segment_number in xrange(1, NUM_SEGMENTS + 1):
            segment = random_string(1)
            segments.append(segment)
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            request_id = uuid.UUID(int=segment_number - 1).hex
            message = RetrieveKeyStart(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                key,
                0, # version number
                segment_number,
            )
            reply = RetrieveKeyStartReply(
                request_id,
                RetrieveKeyStartReply.successful,
                timestamp,
                False,  # is_tombstone
                0,      # version number
                segment_number,
                1,      # num slices
                1,      # slice size
                file_size,
                file_adler32,
                file_md5,
                segment_adler32,
                segment_md5,
                segment
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, exchange
                )].put(reply)
                messages.append((message, exchange))
        return segments, messages, file_size, file_adler32, file_md5

    def test_retrieve_small(self):
        avatar_id = 1001
        timestamp = time.time()
        key = self._key_generator.next()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(avatar_id, timestamp, key)

        num_segments = 8

        retriever = AMQPRetriever(
            self.amqp_handler,
            self.exchange_manager,
            avatar_id,
            key,
            NUM_SEGMENTS,
            num_segments
        )
        retrieved = list(retriever.retrieve(0))

        expected = [
            dict((i + 1, segment)
                 for i, segment in enumerate(segments[:num_segments]))
        ]
        self.assertEqual(retrieved, expected)

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

    # TODO: test when nodes are down
    # TODO: test when receiving replies out of order

    def _make_large_data(self, avatar_id, timestamp, key, n_slices):
        file_size = NUM_SEGMENTS * n_slices
        file_adler32 = -42
        file_md5 = 'ffffff'
        messages = []
        slices = []
        segment_adler32s = {}
        segment_md5s = {}

        slices.append([])
        sequence_number = 0
        for segment_number in xrange(1, NUM_SEGMENTS + 1):
            segment = random_string(1)
            slices[sequence_number].append(segment)
            segment_adler32s[segment_number] = zlib.adler32(segment)
            segment_md5s[segment_number] = hashlib.md5(segment)
            request_id = uuid.UUID(int=segment_number - 1).hex
            message = RetrieveKeyStart(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                key,
                0, # version number
                segment_number,
            )
            reply = RetrieveKeyStartReply(
                request_id,
                RetrieveKeyStartReply.successful,
                timestamp,
                False,  # is_tombstone
                0,      # version number
                segment_number,
                n_slices,
                1,
                file_size,
                file_adler32,
                file_md5,
                # TODO: these should be for the cat'd segments
                segment_adler32s[segment_number],
                segment_md5s[segment_number],
                segment
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, exchange
                )].put(reply)
                messages.append((message, exchange))

        for _ in xrange(n_slices - 2):
            slices.append([])
            sequence_number += 1
            for segment_number in xrange(1, NUM_SEGMENTS + 1):
                segment = random_string(1)
                slices[sequence_number].append(segment)
                segment_adler32s[segment_number] = zlib.adler32(
                    segment,
                    segment_adler32s[segment_number]
                )
                segment_md5s[segment_number].update(segment)
                request_id = uuid.UUID(int=segment_number - 1).hex
                message = RetrieveKeyNext(
                    request_id,
                    sequence_number
                )
                reply = RetrieveKeyNextReply(
                    request_id,
                    RetrieveKeyNextReply.successful,
                    segment
                )
                for exchange in self.exchange_manager[segment_number - 1]:
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, exchange
                    )].put(reply)
                    messages.append((message, exchange))

        slices.append([])
        sequence_number += 1
        for segment_number in xrange(1, NUM_SEGMENTS + 1):
            segment = random_string(1)
            slices[sequence_number].append(segment)
            segment_adler32s[segment_number] = zlib.adler32(
                segment,
                segment_adler32s[segment_number]
            )
            segment_md5s[segment_number].update(segment)
            request_id = uuid.UUID(int=segment_number - 1).hex
            message = RetrieveKeyFinal(
                request_id,
                sequence_number
            )
            reply = RetrieveKeyFinalReply(
                request_id,
                RetrieveKeyFinalReply.successful,
                segment
            )
            for exchange in self.exchange_manager[segment_number - 1]:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, exchange
                )].put(reply)
                messages.append((message, exchange))

        return slices, messages, file_size, file_adler32, file_md5

    def test_retrieve_large(self):
        avatar_id = 1001
        timestamp = time.time()
        key = self._key_generator.next()
        (
            slices,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_large_data(avatar_id, timestamp, key, 4)

        num_segments = 8

        retriever = AMQPRetriever(
            self.amqp_handler,
            self.exchange_manager,
            avatar_id,
            key,
            NUM_SEGMENTS,
            num_segments
        )
        retrieved = list(retriever.retrieve(0))

        expected = [
            dict((i + 1, segment)
                 for i, segment in enumerate(segments[:num_segments]))
            for segments in slices
        ]
        self.assertEqual(retrieved, expected)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'retriever did not send expected messages')


if __name__ == "__main__":
    unittest.main()
