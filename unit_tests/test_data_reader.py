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

from messages.retrieve_key import RetrieveKey
from messages.retrieve_key_reply import RetrieveKeyReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_data_reader")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from unit_tests.archive_util import archive_small_content, \
        archive_large_content

from diyapi_data_reader.diyapi_data_reader_main import \
        _handle_retrieve_key, _handle_key_lookup_reply
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_lookup

class TestDataReader(unittest.TestCase):
    """test message handling in data reader"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_retrieve_small_content(self):
        """test retrieving content that fits in a single message"""
        avatar_id = 1001
        key  = self._key_generator.next()
        original_content = random_string(64 * 1024) 
        archive_small_content(self, avatar_id, key, original_content)

        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        test_routing_key = "reply.routing-key"
        message = RetrieveKey(
            request_id,
            avatar_id,
            test_exchange,
            test_routing_key,
            key, 
        )
        marshalled_message = message.marshall()

        data_writer_state = dict()
        replies = _handle_retrieve_key(
            data_writer_state, marshalled_message
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
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, RetrieveKeyReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.data_content, original_content)

    def test_retrieve_large_content(self):
        """test retrieving content that fits in a multiple messages"""
        segment_size = 120 * 1024
        chunk_count = 10
        total_size = segment_size * chunk_count
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = self._key_generator.next()
        archive_large_content(
            self, avatar_id, key, segment_size, total_size, test_data
        )    

if __name__ == "__main__":
    unittest.main()
