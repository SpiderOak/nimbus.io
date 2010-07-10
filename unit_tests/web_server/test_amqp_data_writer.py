# -*- coding: utf-8 -*-
"""
test_data_writer.py

test diyapi_web_server/amqp_data_writer.py
"""
import unittest
import logging

import gevent

from unit_tests.web_server import util

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply

from diyapi_web_server.exceptions import (
    DataWriterDownError,
    ArchiveFailedError,
    HandoffFailedError,
    StartHandoff,
)
from diyapi_web_server.amqp_data_writer import AMQPDataWriter


class TestAMQPDataWriter(unittest.TestCase):
    """test diyapi_web_server/amqp_data_writer.py"""
    def setUp(self):
        self.log = logging.getLogger('TestAMQPDataWriter')
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange = 'test_exchange'
        self.writer = AMQPDataWriter(self.amqp_handler, self.exchange)
        self.handoff_writer = AMQPDataWriter(self.amqp_handler,
                                             'other_exchange')

    def test_hash(self):
        self.assertEquals(hash(self.writer), hash(self.exchange))

    def test_eq(self):
        self.assertEquals(
            self.writer, AMQPDataWriter(self.amqp_handler, self.exchange))
        self.assertNotEquals(
            self.writer, AMQPDataWriter(self.amqp_handler, 'other-exchange'))
        self.assertNotEquals(self.writer, 5)

    def test_archive_key_entire(self):
        self.log.debug('test_archive_key_entire')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        key = 'key'
        version_number = 0
        segment_number = 1
        file_adler32 = 1
        file_md5 = 'ffff'
        segment_adler32 = 1
        segment_md5 = 'ffff'
        segment = 'segment'
        previous_size = 0
        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            version_number,
            segment_number,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        reply = ArchiveKeyFinalReply(
            request_id,
            ArchiveKeyFinalReply.successful,
            previous_size
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.writer.archive_key_entire(
            request_id,
            avatar_id,
            timestamp,
            key,
            version_number,
            segment_number,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        self.assertEqual(result, previous_size)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_archive_key_entire_with_handoff(self):
        self.log.debug('test_archive_key_entire_with_handoff')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        key = 'key'
        version_number = 0
        segment_number = 1
        file_adler32 = 1
        file_md5 = 'ffff'
        segment_adler32 = 1
        segment_md5 = 'ffff'
        segment = 'segment'
        previous_size = 0
        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            version_number,
            segment_number,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        reply = ArchiveKeyFinalReply(
            request_id,
            ArchiveKeyFinalReply.successful,
            previous_size
        )
        self.amqp_handler.replies_to_send_by_exchange[
            request_id,
            self.handoff_writer.exchange
        ].put(reply)
        task = gevent.spawn(
            self.writer.archive_key_entire,
            request_id,
            avatar_id,
            timestamp,
            key,
            version_number,
            segment_number,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        gevent.sleep(0)
        task.kill(StartHandoff([self.handoff_writer]))
        self.assertEqual(task.value, previous_size)
        expected = [
            (message.marshall(), exchange)
            for exchange in (self.exchange, self.handoff_writer.exchange)
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_archive_key_entire_with_error(self):
        self.log.debug('test_archive_key_entire_with_error')
        reply = ArchiveKeyFinalReply(
            'request_id',
            ArchiveKeyFinalReply.error_exception,
            error_message='there was an error'
        )
        self.amqp_handler.replies_to_send['request_id'].put(reply)
        self.assertRaises(
            ArchiveFailedError,
            self.writer.archive_key_entire,
            'request_id',
            1001,
            12345,
            'key',
            0,
            1,
            1,
            'ffff',
            1,
            'ffff',
            'segment'
        )

    def test_archive_key_start(self):
        self.log.debug('test_archive_key_start')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        sequence_number = 0
        key = 'key'
        version_number = 0
        segment_number = 1
        segment = 'segment'
        message = ArchiveKeyStart(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            sequence_number,
            key,
            version_number,
            segment_number,
            len(segment),
            segment
        )
        reply = ArchiveKeyStartReply(
            request_id,
            ArchiveKeyStartReply.successful,
            0
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        self.writer.archive_key_start(
            request_id,
            avatar_id,
            timestamp,
            sequence_number,
            key,
            version_number,
            segment_number,
            segment
        )
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_archive_key_start_with_handoff(self):
        self.log.debug('test_archive_key_start_with_handoff')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        sequence_number = 0
        key = 'key'
        version_number = 0
        segment_number = 1
        segment = 'segment'
        message = ArchiveKeyStart(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            sequence_number,
            key,
            version_number,
            segment_number,
            len(segment),
            segment
        )
        reply = ArchiveKeyStartReply(
            request_id,
            ArchiveKeyStartReply.successful,
            0
        )
        self.amqp_handler.replies_to_send_by_exchange[
            request_id,
            self.handoff_writer.exchange
        ].put(reply)
        task = gevent.spawn(
            self.writer.archive_key_start,
            request_id,
            avatar_id,
            timestamp,
            sequence_number,
            key,
            version_number,
            segment_number,
            segment
        )
        gevent.sleep(0)
        task.kill(StartHandoff([self.handoff_writer]))
        expected = [
            (message.marshall(), exchange)
            for exchange in (self.exchange, self.handoff_writer.exchange)
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_archive_key_next(self):
        self.log.debug('test_archive_key_next')
        request_id = 'request_id'
        sequence_number = 0
        segment = 'segment'
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
        self.amqp_handler.replies_to_send[request_id].put(reply)
        self.writer.archive_key_next(
            request_id,
            sequence_number,
            segment
        )
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_archive_key_final(self):
        self.log.debug('test_archive_key_final')
        request_id = 'request_id'
        sequence_number = 0
        file_size = 123
        file_adler32 = 1
        file_md5 = 'ffff'
        segment_adler32 = 1
        segment_md5 = 'ffff'
        segment = 'segment'
        previous_size = 0
        message = ArchiveKeyFinal(
            request_id,
            sequence_number,
            file_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        reply = ArchiveKeyFinalReply(
            request_id,
            ArchiveKeyFinalReply.successful,
            previous_size
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.writer.archive_key_final(
            request_id,
            sequence_number,
            file_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            segment
        )
        self.assertEqual(result, previous_size)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_hinted_handoff(self):
        self.log.debug('test_hinted_handoff')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        key = 'key'
        version_number = 0
        segment_number = 0
        dest_exchange = 'other-exchange'
        previous_size = 0
        message = HintedHandoff(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            version_number,
            segment_number,
            dest_exchange,
        )
        reply = HintedHandoffReply(
            request_id,
            HintedHandoffReply.successful,
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.writer.hinted_handoff(
            request_id,
            avatar_id,
            timestamp,
            key,
            version_number,
            segment_number,
            dest_exchange
        )
        self.assertEqual(result, previous_size)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_hinted_handoff_with_error(self):
        self.log.debug('test_hinted_handoff_with_error')
        reply = HintedHandoffReply(
            'request_id',
            HintedHandoffReply.error_exception,
            error_message='there was an error'
        )
        self.amqp_handler.replies_to_send['request_id'].put(reply)
        self.assertRaises(
            HandoffFailedError,
            self.writer.hinted_handoff,
            'request_id',
            1001,
            12345,
            'key',
            0,
            1,
            'other-exchange'
        )
        self.assertEqual(len(self.amqp_handler.messages), 1)


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
