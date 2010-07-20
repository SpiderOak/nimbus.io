# -*- coding: utf-8 -*-
"""
test_retriever.py

test diyapi_web_server/retriever.py
"""
import os
import unittest
import uuid
import hashlib
import zlib
import logging

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util

from diyapi_web_server.amqp_data_reader import AMQPDataReader
from diyapi_web_server.exceptions import RetrieveFailedError

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply

from diyapi_web_server.retriever import Retriever


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
NUM_SEGMENTS = 10
SEGMENTS_NEEDED = 8


class TestRetriever(unittest.TestCase):
    """test diyapi_web_server/retriever.py"""
    def setUp(self):
        self.amqp_handler = util.FakeAMQPHandler()
        self.data_readers = [AMQPDataReader(self.amqp_handler, exchange)
                             for exchange in EXCHANGES]
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next
        self.log = logging.getLogger('TestRetriever')

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def _make_small_data(self, avatar_id, timestamp, key):
        file_size = NUM_SEGMENTS
        file_adler32 = -42
        file_md5 = 'ffffff'
        messages = []
        segments = []
        for i in xrange(NUM_SEGMENTS):
            segment_number = i + 1
            segment = random_string(1)
            segments.append(segment)
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            request_id = uuid.UUID(int=i).hex
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
            data_reader = self.data_readers[i]
            if not data_reader.is_down:
                messages.append((message, data_reader.exchange))
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_reader.exchange
                )].put(reply)

        return segments, messages, file_size, file_adler32, file_md5

    def test_retrieve_small(self):
        self.log.debug('test_retrieve_small')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(avatar_id, timestamp, key)

        retriever = Retriever(
            self.data_readers,
            avatar_id,
            key,
            SEGMENTS_NEEDED
        )
        retrieved = list(retriever.retrieve(0))

        expected = [
            dict((i + 1, segment)
                 for i, segment in enumerate(segments[:SEGMENTS_NEEDED]))
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

    def test_retrieve_small_when_exchange_is_down(self):
        self.log.debug('test_retrieve_small_when_exchange_is_down')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_readers[0].mark_down()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(avatar_id, timestamp, key)

        retriever = Retriever(
            self.data_readers,
            avatar_id,
            key,
            SEGMENTS_NEEDED
        )
        retrieved = list(retriever.retrieve(0))

        expected = [
            dict((i + 2, segment)
                 for i, segment in enumerate(segments[1:SEGMENTS_NEEDED + 1]))
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
        for i in xrange(NUM_SEGMENTS):
            segment_number = i + 1
            segment = random_string(1)
            slices[sequence_number].append(segment)
            segment_adler32s[segment_number] = zlib.adler32(segment)
            segment_md5s[segment_number] = hashlib.md5(segment)
            request_id = uuid.UUID(int=i).hex
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
            data_reader = self.data_readers[i]
            messages.append((message, data_reader.exchange))
            if not data_reader.is_down:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_reader.exchange
                )].put(reply)

        for _ in xrange(n_slices - 2):
            slices.append([])
            sequence_number += 1
            for i in xrange(NUM_SEGMENTS):
                segment_number = i + 1
                segment = random_string(1)
                slices[sequence_number].append(segment)
                segment_adler32s[segment_number] = zlib.adler32(
                    segment,
                    segment_adler32s[segment_number]
                )
                segment_md5s[segment_number].update(segment)
                request_id = uuid.UUID(int=i).hex
                message = RetrieveKeyNext(
                    request_id,
                    sequence_number
                )
                reply = RetrieveKeyNextReply(
                    request_id,
                    sequence_number,
                    RetrieveKeyNextReply.successful,
                    segment
                )
                data_reader = self.data_readers[i]
                messages.append((message, data_reader.exchange))
                if not data_reader.is_down:
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, data_reader.exchange
                    )].put(reply)

        slices.append([])
        sequence_number += 1
        for i in xrange(NUM_SEGMENTS):
            segment_number = i + 1
            segment = random_string(1)
            slices[sequence_number].append(segment)
            segment_adler32s[segment_number] = zlib.adler32(
                segment,
                segment_adler32s[segment_number]
            )
            segment_md5s[segment_number].update(segment)
            request_id = uuid.UUID(int=i).hex
            message = RetrieveKeyFinal(
                request_id,
                sequence_number
            )
            reply = RetrieveKeyFinalReply(
                request_id,
                sequence_number,
                RetrieveKeyFinalReply.successful,
                segment
            )
            data_reader = self.data_readers[i]
            messages.append((message, data_reader.exchange))
            if not data_reader.is_down:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_reader.exchange
                )].put(reply)

        return slices, messages, file_size, file_adler32, file_md5

    def test_retrieve_large(self):
        self.log.debug('test_retrieve_large')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        (
            slices,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_large_data(avatar_id, timestamp, key, 4)

        retriever = Retriever(
            self.data_readers,
            avatar_id,
            key,
            SEGMENTS_NEEDED
        )
        retrieved = list(retriever.retrieve(0))

        expected = [
            dict((i + 1, segment)
                 for i, segment in enumerate(segments[:SEGMENTS_NEEDED]))
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

    def test_retrieve_nonexistent(self):
        self.log.debug('test_retrieve_nonexistent')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()

        for segment_number in xrange(1, NUM_SEGMENTS + 1):
            request_id = uuid.UUID(int=segment_number - 1).hex
            reply = RetrieveKeyStartReply(
                request_id,
                RetrieveKeyStartReply.error_key_not_found,
                error_message='key not found',
            )
            data_reader = self.data_readers[segment_number - 1]
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, data_reader.exchange
            )].put(reply)

        retriever = Retriever(
            self.data_readers,
            avatar_id,
            key,
            SEGMENTS_NEEDED
        )

        self.assertRaises(RetrieveFailedError, list, retriever.retrieve(0))


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
