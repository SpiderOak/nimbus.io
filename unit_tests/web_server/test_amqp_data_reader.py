# -*- coding: utf-8 -*-
"""
test_data_reader.py

test diyapi_web_server/amqp_data_reader.py
"""
import unittest
import logging

from unit_tests.web_server import util

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.database_listmatch import DatabaseListMatch
from messages.database_listmatch_reply import DatabaseListMatchReply
from messages.space_usage import SpaceUsage
from messages.space_usage_reply import SpaceUsageReply

from diyapi_web_server.amqp_data_reader import AMQPDataReader

from diyapi_web_server.exceptions import (
    DataReaderDownError,
    RetrieveFailedError,
)


class TestAMQPDataReader(unittest.TestCase):
    """test diyapi_web_server/amqp_data_reader.py"""
    def setUp(self):
        self.log = logging.getLogger('TestAMQPDataReader')
        self.amqp_handler = util.FakeAMQPHandler()
        self.exchange = 'test_exchange'
        self.reader = AMQPDataReader(self.amqp_handler, self.exchange)

    def test_retrieve_key_start(self):
        self.log.debug('test_retrieve_key_start')
        request_id = 'request_id'
        avatar_id = 1001
        key = 'key'
        version_number = 0
        segment_number = 1
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            key,
            version_number,
            segment_number
        )
        reply = RetrieveKeyStartReply(
            request_id,
            RetrieveKeyStartReply.successful,
            12345,  # timestamp
            False,  # is_tombstone
            version_number,
            segment_number,
            1,      # num slices
            1,      # slice size
            23,     # file_size
            42,     # file_adler32
            'fff',  # file_md5
            36,     # segment_adler32
            'abc',  # segment_md5
            'segment'
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.reader.retrieve_key_start(
            request_id,
            avatar_id,
            key,
            version_number,
            segment_number,
        )
        self.assertEqual(result, reply)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_retrieve_key_start_with_error(self):
        self.log.debug('test_retrieve_key_start_with_error')
        reply = RetrieveKeyStartReply(
            'request_id',
            RetrieveKeyStartReply.error_exception,
            error_message='there was an error'
        )
        self.amqp_handler.replies_to_send['request_id'].put(reply)
        self.assertRaises(
            RetrieveFailedError,
            self.reader.retrieve_key_start,
            'request_id',
            1001,
            'key',
            0,
            3
        )

    def test_retrieve_key_start_when_down_raises_error(self):
        self.log.debug('test_retrieve_key_start_when_down_raises_error')
        self.reader.mark_down()
        self.assertRaises(
            DataReaderDownError,
            self.reader.retrieve_key_start,
            'request_id',
            1001,
            'key',
            0,
            3
        )

    def test_retrieve_key_next(self):
        self.log.debug('test_retrieve_key_next')
        request_id = 'request_id'
        sequence_number = 2
        segment = 'segment'
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
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.reader.retrieve_key_next(
            request_id,
            sequence_number
        )
        self.assertEqual(result, segment)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    def test_retrieve_key_final(self):
        self.log.debug('test_retrieve_key_final')
        request_id = 'request_id'
        sequence_number = 2
        segment = 'segment'
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
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.reader.retrieve_key_final(
            request_id,
            sequence_number
        )
        self.assertEqual(result, segment)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    # listmatch is not technically part of data_reader
    def test_listmatch(self):
        self.log.debug('test_listmatch')
        request_id = 'request_id'
        avatar_id = 1001
        prefix = 'prefix'
        key_list = ['key1', 'key2']
        message = DatabaseListMatch(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name,
            prefix
        )
        reply = DatabaseListMatchReply(
            request_id,
            DatabaseListMatchReply.successful,
            key_list=key_list
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.reader.listmatch(
            request_id,
            avatar_id,
            prefix
        )
        self.assertEqual(result, key_list)
        self.assertEqual(len(self.amqp_handler.messages), 1)
        actual = (self.amqp_handler.messages[0][0].marshall(),
                  self.amqp_handler.messages[0][1])
        expected = (message.marshall(), self.exchange)
        self.assertEqual(
            actual, expected, 'did not send expected messages')

    # get_space_usage is not technically part of data_reader
    def test_get_space_usage(self):
        self.log.debug('test_get_space_usage')
        request_id = 'request_id'
        avatar_id = 1001
        message = SpaceUsage(
            request_id,
            avatar_id,
            self.amqp_handler.exchange,
            self.amqp_handler.queue_name
        )
        reply = SpaceUsageReply(
            request_id,
            SpaceUsageReply.successful
        )
        self.amqp_handler.replies_to_send[request_id].put(reply)
        result = self.reader.get_space_usage(
            request_id,
            avatar_id
        )
        #self.assertEqual(result, key_list)
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
