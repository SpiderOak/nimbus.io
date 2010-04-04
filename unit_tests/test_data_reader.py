# -*- coding: utf-8 -*-
"""
test_data_reader.py

test the data reader process
"""
import logging
import os
import os.path
import shutil
import unittest
import uuid

from tools.standard_logging import initialize_logging
from tools import amqp_connection

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from unit_tests.archive_util import archive_small_content, \
        archive_large_content

from diyapi_data_reader.diyapi_data_reader_main import \
        _handle_retrieve_key_start, \
        _handle_retrieve_key_next, \
        _handle_retrieve_key_final, \
        _handle_key_lookup_reply
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_lookup

_key_generator = generate_key()
_reply_routing_header = "test_data_reader"

class TestDataReader(unittest.TestCase):
    """test message handling in data reader"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def _retrieve(self, avatar_id, key, segment_number):
        """retrieve content for a key"""
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            key,
            segment_number
        )
        marshalled_message = message.marshall()

        data_reader_state = dict()
        replies = _handle_retrieve_key_start(
            data_reader_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the data reader to send a
        # database_key_lookup to the database server
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply_routing_key, DatabaseKeyLookup.routing_key)
        self.assertEqual(reply.__class__, DatabaseKeyLookup)
        self.assertEqual(reply.request_id, request_id)

        # hand off the reply to the database server
        marshalled_message = reply.marshall()
        database_state = {_database_cache : dict()}
        replies = _handle_key_lookup(database_state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0)
        self.assertTrue(reply.key_found)

        # pass the database server reply back to data_reader
        # we should get a reply we can send to the web api 
        marshalled_message = reply.marshall()
        replies = _handle_key_lookup_reply(
            data_reader_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, RetrieveKeyStartReply)
        self.assertEqual(reply.result, 0)

        segment_count = reply.segment_count

        # if the file only has one segment, we are done
        if  segment_count == 1:
            return reply.data_content

        content_list = [reply.data_content]

        # we have segment 0, get segments 1..N-1
        for sequence in range(1, segment_count-1):
            message = RetrieveKeyNext(request_id, sequence)
            marshalled_message = message.marshall()
            replies = _handle_retrieve_key_next(
                data_reader_state, marshalled_message
            )
            self.assertEqual(len(replies), 1)
            [(reply_exchange, reply_routing_key, reply, ), ] = replies
            self.assertEqual(reply.__class__, RetrieveKeyNextReply)
            self.assertEqual(reply.result, 0)
            content_list.append(reply.data_content)

        # get the last segment
        sequence = segment_count - 1
        message = RetrieveKeyFinal(request_id, sequence)
        marshalled_message = message.marshall()
        replies = _handle_retrieve_key_final(
            data_reader_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, RetrieveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        content_list.append(reply.data_content)

        return "".join(content_list)

    def test_retrieve_small_content(self):
        """test retrieving content that fits in a single message"""
        avatar_id = 1001
        key  = _key_generator.next()
        segment_number = 5
        original_content = random_string(64 * 1024) 

        archive_small_content(
            self, avatar_id, key, segment_number, original_content
        )
        test_content = self._retrieve(avatar_id, key, segment_number)

        self.assertEqual(test_content, original_content)

    def test_retrieve_large_content(self):
        """test retrieving content that fits in a multiple messages"""
        segment_size = 120 * 1024
        chunk_count = 10
        total_size = segment_size * chunk_count
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = _key_generator.next()
        segment_number = 5
        archive_large_content(
            self, 
            avatar_id, 
            key, 
            segment_number, 
            segment_size, 
            total_size, 
            test_data
        )    

        expected_content = "".join(test_data)
        test_content = self._retrieve(avatar_id, key, segment_number)
        self.assertEqual(test_content, expected_content)

    def test_retrieve_large_content_short_last_segment(self):
        """
        test retrieving content that fits in a multiple messages
        wiht the last segment samller than the others
        """
        segment_size = 120 * 1024
        short_size = 1024
        chunk_count = 10
        total_size = (segment_size * (chunk_count-1)) + short_size 
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count-1)]
        test_data.append(random_string(short_size))
        key  = _key_generator.next()
        segment_number = 5
        archive_large_content(
            self, 
            avatar_id, 
            key, 
            segment_number, 
            segment_size, 
            total_size, 
            test_data
        )    

        expected_content = "".join(test_data)
        test_content = self._retrieve(avatar_id, key, segment_number)
        self.assertEqual(test_content, expected_content)

    def test_retrieve_large_content_2_segments(self):
        """
        test retrieving content that fits in a multiple messages
        but without a 'middle' i.e. no RetrieveKeyNext
        """
        segment_size = 120 * 1024
        chunk_count = 2
        total_size = segment_size * chunk_count
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = _key_generator.next()
        segment_number = 5
        archive_large_content(
            self, 
            avatar_id, 
            key, 
            segment_number,
            segment_size, 
            total_size, 
            test_data
        )    

        expected_content = "".join(test_data)
        test_content = self._retrieve(avatar_id, key, segment_number)
        self.assertEqual(test_content, expected_content)


if __name__ == "__main__":
    unittest.main()
