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
from messages.destroy_key import DestroyKey
from messages.destroy_key_reply import DestroyKeyReply
from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply
from messages.process_status import ProcessStatus

from diyapi_web_server.exceptions import (
    DataWriterDownError,
    ArchiveFailedError,
    HandoffFailedError,
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

    def test_archive_key_entire(self):
        self.log.debug('test_archive_key_entire')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        key = 'key'
        version_number = 0
        segment_number = 1
        total_size = 42
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
            total_size,
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
            total_size,
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
            123,
            1,
            'ffff',
            1,
            'ffff',
            'segment'
        )

    def test_archive_key_entire_when_down_raises_error(self):
        self.log.debug('test_archive_key_entire_when_down_raises_error')
        self.writer.mark_down()
        self.assertRaises(
            DataWriterDownError,
            self.writer.archive_key_entire,
            'request_id',
            1001,
            12345,
            'key',
            0,
            1,
            123,
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

    def test_shutdown(self):
        self.log.debug('test_shutdown')
        self.assertFalse(self.writer.is_down)
        timestamp = 12345
        exchange = 'some_exchange'
        routing_header = 'a_routing_header'
        message = ProcessStatus(
            timestamp,
            exchange,
            routing_header,
            ProcessStatus.status_shutdown
        )
        self.amqp_handler._call_subscriptions(message)
        self.assertTrue(self.writer.is_down)

    def test_startup(self):
        self.log.debug('test_startup')
        self.writer.mark_down()
        timestamp = 12345
        exchange = 'some_exchange'
        routing_header = 'a_routing_header'
        message = ProcessStatus(
            timestamp,
            exchange,
            routing_header,
            ProcessStatus.status_startup
        )
        self.amqp_handler._call_subscriptions(message)
        self.assertFalse(self.writer.is_down)

    def test_heartbeat_coming_up(self):
        self.log.debug('test_heartbeat_coming_up')
        self.writer.mark_down()
        timestamp = 12345
        exchange = 'some_exchange'
        routing_header = 'a_routing_header'
        message = ProcessStatus(
            timestamp,
            exchange,
            routing_header,
            ProcessStatus.status_heartbeat
        )
        self.amqp_handler._call_subscriptions(message)
        self.assertFalse(self.writer.is_down)

    def test_heartbeat_going_down(self):
        self.log.debug('test_heartbeat_going_down')
        self.assertFalse(self.writer.is_down)
        self.writer.heartbeat_interval = 0
        timestamp = 12345
        exchange = 'some_exchange'
        routing_header = 'a_routing_header'
        message = ProcessStatus(
            timestamp,
            exchange,
            routing_header,
            ProcessStatus.status_heartbeat
        )
        self.amqp_handler._call_subscriptions(message)
        gevent.sleep(0)
        self.assertTrue(self.writer.is_down)

    def test_destroy_key(self):
        self.log.debug('test_destroy_key')
        request_id = 'request_id'
        avatar_id = 1001
        timestamp = 12345
        key = 'key'
        version_number = 0
        segment_number = 1
        size_deleted = 432
        message = DestroyKey(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            timestamp,
            key,
            segment_number,
            version_number
        )
        reply = DestroyKeyReply(
            request_id,
            DestroyKeyReply.successful,
            size_deleted
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.writer.destroy_key(
            request_id,
            avatar_id,
            timestamp,
            key,
            segment_number,
            version_number
        )
        self.assertEqual(result, size_deleted)
        self.assertEqual(len(self.amqp_handler.messages), 1)
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
