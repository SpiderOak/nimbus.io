# -*- coding: utf-8 -*-
"""
test_data_reader.py

test the data reader process
"""
import hashlib
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid
import zlib

from tools.standard_logging import initialize_logging
from tools import amqp_connection

from messages.retrieve_key import RetrieveKey
from messages.retrieve_key_reply import RetrieveKeyReply
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_entire_reply import ArchiveKeyEntireReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_data_reader")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from diyapi_data_reader.diyapi_data_reader_main import \
        _handle_retrieve_key, _handle_key_lookup_reply
from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_archive_key_entire, _handle_key_insert_reply
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_insert

class TestDataReader(unittest.TestCase):
    """test message handling in data reader"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_repository_path):
            shutil.rmtree(_repository_path)

    def _archive_content(self, avatar_id, key, content):
        """utility function to set up retrieve"""
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        test_routing_key = "reply.routing-key"
        timestamp = time.time()
        segment_number = 3
        adler32 = zlib.adler32(content)
        md5 = hashlib.md5(content).digest()
        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            test_exchange,
            test_routing_key,
            key, 
            timestamp,
            segment_number,
            adler32,
            md5,
            content
        )
        marshalled_message = message.marshall()

        data_writer_state = dict()
        replies = _handle_archive_key_entire(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # after a successful write, we expect the data writer to send a
        # database_key_insert to the database server
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply_routing_key, DatabaseKeyInsert.routing_key)
        self.assertEqual(reply.__class__, DatabaseKeyInsert)
        self.assertEqual(reply.request_id, request_id)

        # hand off the reply to the database server
        marshalled_message = reply.marshall()
        database_state = {_database_cache : dict()}
        replies = _handle_key_insert(database_state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        # pass the database server reply back to data_writer
        # we should get a reply we can send to the web api 
        marshalled_message = reply.marshall()
        replies = _handle_key_insert_reply(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, ArchiveKeyEntireReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

    def test_retrieve_small_content(self):
        """test retrieving content that fits in a single message"""
        avatar_id = 1001
        key  = self._key_generator.next()
        original_content = random_string(64 * 1024) 
        self._archive_content(avatar_id, key, original_content)

if __name__ == "__main__":
    unittest.main()
