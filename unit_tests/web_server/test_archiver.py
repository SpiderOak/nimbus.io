# -*- coding: utf-8 -*-
"""
test_archiver.py

test diyapi_web_server/archiver.py
"""
import os
import unittest
import uuid
import hashlib
import zlib
import logging

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util

from diyapi_web_server.amqp_data_writer import AMQPDataWriter
from diyapi_web_server.exceptions import ArchiveFailedError

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply

from diyapi_web_server.archiver import Archiver


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
NUM_SEGMENTS = 10


class TestArchiver(unittest.TestCase):
    """test diyapi_web_server/archiver.py"""
    def setUp(self):
        self.amqp_handler = util.FakeAMQPHandler()
        self.data_writers = [AMQPDataWriter(self.amqp_handler, exchange)
                             for exchange in EXCHANGES]
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next
        self.log = logging.getLogger('TestArchiver')

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def _make_small_data(self, avatar_id, timestamp, key, fail=False,
                         reply_result=None, error_message=None,
                         handoff_fail=False,
                         handoff_reply_result=None,
                         handoff_error_message=None):
        file_size = 1024 * NUM_SEGMENTS
        file_adler32 = -42
        file_md5 = 'ffffff'
        messages = []
        messages_to_append = []
        segments = []
        handoff_messages = []
        for i in xrange(NUM_SEGMENTS):
            segment_number = i + 1
            segment = random_string(1024)
            segments.append(segment)
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            request_id = uuid.UUID(int=i).hex
            message = ArchiveKeyEntire(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                key,
                0, # version number
                segment_number,
                file_size,
                file_adler32,
                file_md5,
                segment_adler32,
                segment_md5,
                segment
            )
            if reply_result is None:
                reply = ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                )
            else:
                reply = ArchiveKeyFinalReply(
                    request_id,
                    reply_result,
                    0,
                    error_message
                )
            data_writer = self.data_writers[i]
            messages.append((message, data_writer.exchange))
            if data_writer.is_down:
                for handoff_data_writer in self.data_writers[1:3]:
                    if not fail:
                        self.amqp_handler.replies_to_send_by_exchange[(
                            request_id, handoff_data_writer.exchange
                        )].put(reply)
                        handoff = HintedHandoff(
                            request_id,
                            avatar_id,
                            self.amqp_handler.exchange,
                            self.amqp_handler.queue_name,
                            timestamp,
                            key,
                            0, # version number
                            segment_number,
                            data_writer.exchange,
                        )
                        handoff_messages.append((
                            handoff, handoff_data_writer.exchange))
                        if not handoff_fail:
                            if handoff_reply_result is None:
                                handoff_reply = HintedHandoffReply(
                                    request_id,
                                    HintedHandoffReply.successful,
                                )
                            else:
                                handoff_reply = HintedHandoffReply(
                                    request_id,
                                    handoff_reply_result,
                                    error_message=handoff_error_message
                                )
                            self.amqp_handler.replies_to_send_by_exchange[(
                                request_id, handoff_data_writer.exchange
                            )].put(handoff_reply)
                    messages_to_append.append((
                        message, handoff_data_writer.exchange))
            else:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_writer.exchange
                )].put(reply)
        messages.extend(messages_to_append)
        messages.extend(handoff_messages)
        return segments, messages, file_size, file_adler32, file_md5

    def test_archive_small(self):
        self.log.debug('test_archive_small')
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

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )
        previous_size = archiver.archive_final(
            file_size,
            file_adler32,
            file_md5,
            segments,
            0
        )

        self.assertEqual(previous_size, 0)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'archiver did not send expected messages')

    def test_archive_small_with_handoff(self):
        self.log.debug('test_archive_small_with_handoff')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_writers[0].mark_down()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(avatar_id, timestamp, key)
        self.data_writers[0].mark_up()

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        previous_size = archiver.archive_final(
            file_size,
            file_adler32,
            file_md5,
            segments,
            0
        )

        self.assertEqual(previous_size, 0)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'archiver did not send expected messages')

    def test_archive_small_with_failure(self):
        self.log.debug('test_archive_small_with_failure')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_writers[0].mark_down()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(avatar_id, timestamp, key, True)
        self.data_writers[0].mark_up()

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        self.assertRaises(
            ArchiveFailedError,
            archiver.archive_final,
            file_size,
            file_adler32,
            file_md5,
            segments,
            0
        )

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'archiver did not send expected messages')

    def test_archive_small_with_handoff_failure(self):
        self.log.debug('test_archive_small_with_handoff_failure')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_writers[0].mark_down()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(avatar_id, timestamp, key, handoff_fail=True)
        self.data_writers[0].mark_up()

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        self.assertRaises(
            ArchiveFailedError,
            archiver.archive_final,
            file_size,
            file_adler32,
            file_md5,
            segments,
            0
        )

    def test_archive_small_with_error(self):
        self.log.debug('test_archive_small_with_error')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(
            avatar_id, timestamp, key,
            reply_result=ArchiveKeyFinalReply.error_exception,
            error_message='There was an exception')

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        self.assertRaises(
            ArchiveFailedError,
            archiver.archive_final,
            file_size,
            file_adler32,
            file_md5,
            segments,
            0
        )

    def test_archive_small_with_handoff_error(self):
        self.log.debug('test_archive_small_with_handoff_error')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_writers[0].mark_down()
        (
            segments,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_small_data(
            avatar_id, timestamp, key,
            handoff_reply_result=HintedHandoffReply.error_exception,
            handoff_error_message='There was an exception')
        self.data_writers[0].mark_up()

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        self.assertRaises(
            ArchiveFailedError,
            archiver.archive_final,
            file_size,
            file_adler32,
            file_md5,
            segments,
            0
        )

    def _make_large_data(self, avatar_id, timestamp,
                         key, n_slices, fail=False):
        file_size = NUM_SEGMENTS * n_slices
        file_adler32 = -42
        file_md5 = 'ffffff'
        messages = []
        messages_to_append = []
        slices = []
        segment_adler32s = {}
        segment_md5s = {}
        handoff_messages = []

        slices.append([])
        sequence_number = 0
        for i in xrange(NUM_SEGMENTS):
            segment_number = i + 1
            segment = random_string(1)
            slices[sequence_number].append(segment)
            segment_adler32s[segment_number] = zlib.adler32(segment)
            segment_md5s[segment_number] = hashlib.md5(segment)
            request_id = uuid.UUID(int=i).hex
            message = ArchiveKeyStart(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                sequence_number,
                key,
                0, # version number
                segment_number,
                len(segment),
                segment
            )
            reply = ArchiveKeyStartReply(
                request_id,
                ArchiveKeyStartReply.successful,
                0
            )
            data_writer = self.data_writers[i]
            messages.append((message, data_writer.exchange))
            if data_writer.is_down:
                for handoff_data_writer in self.data_writers[1:3]:
                    if not fail:
                        self.amqp_handler.replies_to_send_by_exchange[(
                            request_id, handoff_data_writer.exchange
                        )].put(reply)
                    messages_to_append.append((
                        message, handoff_data_writer.exchange))
            else:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_writer.exchange
                )].put(reply)

        messages.extend(messages_to_append)

        if fail:
            return slices, messages, file_size, file_adler32, file_md5

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
                message = ArchiveKeyNext(
                    request_id,
                    sequence_number,
                    segment
                )
                reply = ArchiveKeyNextReply(
                    request_id,
                    ArchiveKeyNextReply.successful,
                    0
                )
                data_writer = self.data_writers[i]
                if data_writer.is_down:
                    for handoff_data_writer in self.data_writers[1:3]:
                        messages.append((
                            message, handoff_data_writer.exchange))
                        self.amqp_handler.replies_to_send_by_exchange[(
                            request_id, handoff_data_writer.exchange
                        )].put(reply)
                else:
                    messages.append((message, data_writer.exchange))
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, data_writer.exchange
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
            message = ArchiveKeyFinal(
                request_id,
                sequence_number,
                file_size,
                file_adler32,
                file_md5,
                segment_adler32s[segment_number],
                segment_md5s[segment_number].digest(),
                segment
            )
            reply = ArchiveKeyFinalReply(
                request_id,
                ArchiveKeyFinalReply.successful,
                0
            )
            data_writer = self.data_writers[i]
            if data_writer.is_down:
                for handoff_data_writer in self.data_writers[1:3]:
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, handoff_data_writer.exchange
                    )].put(reply)
                    handoff = HintedHandoff(
                        request_id,
                        avatar_id,
                        self.amqp_handler.exchange,
                        self.amqp_handler.queue_name,
                        timestamp,
                        key,
                        0, # version number
                        segment_number,
                        data_writer.exchange,
                    )
                    handoff_reply = HintedHandoffReply(
                        request_id,
                        HintedHandoffReply.successful,
                    )
                    handoff_messages.append((
                        handoff, handoff_data_writer.exchange))
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, handoff_data_writer.exchange
                    )].put(handoff_reply)
                    messages.append((message, handoff_data_writer.exchange))
            else:
                messages.append((message, data_writer.exchange))
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, data_writer.exchange
                )].put(reply)

        messages.extend(handoff_messages)

        return slices, messages, file_size, file_adler32, file_md5

    def test_archive_large(self):
        self.log.debug('test_archive_large')
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

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        for segments in slices[:-1]:
            archiver.archive_slice(segments, 0)

        previous_size = archiver.archive_final(
            file_size,
            file_adler32,
            file_md5,
            slices[-1],
            0
        )

        self.assertEqual(previous_size, 0)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'archiver did not send expected messages')

    def test_archive_large_with_handoff(self):
        self.log.debug('test_archive_large_with_handoff')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_writers[0].mark_down()
        (
            slices,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_large_data(avatar_id, timestamp, key, 4)
        self.data_writers[0].mark_up()

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        for segments in slices[:-1]:
            archiver.archive_slice(segments, 0)

        previous_size = archiver.archive_final(
            file_size,
            file_adler32,
            file_md5,
            slices[-1],
            0
        )

        self.assertEqual(previous_size, 0)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'archiver did not send expected messages')

    def test_archive_large_with_failure(self):
        self.log.debug('test_archive_large_with_failure')
        avatar_id = 1001
        timestamp = util.fake_time()
        key = self._key_generator.next()
        self.data_writers[0].mark_down()
        (
            slices,
            messages,
            file_size,
            file_adler32,
            file_md5,
        ) = self._make_large_data(avatar_id, timestamp, key, 4, True)
        self.data_writers[0].mark_up()

        archiver = Archiver(
            self.data_writers,
            avatar_id,
            key,
            timestamp
        )

        self.assertRaises(
            ArchiveFailedError,
            archiver.archive_slice,
            slices[0],
            0
        )


        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'archiver did not send expected messages')


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
