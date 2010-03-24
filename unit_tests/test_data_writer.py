# -*- coding: utf-8 -*-
"""
test_data_writer.py

test the data writer process
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

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_entire_reply import ArchiveKeyEntireReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_data_writer")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_archive_key_entire, _handle_key_insert_reply
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_insert

class TestDataWriter(unittest.TestCase):
    """test message handling in data writer"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_archive_key_entire(self):
        """test archiving all data for a key in a single message"""
        content = random_string(64 * 1024) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        test_exchange = "reply-exchange"
        test_routing_key = "reply.routing-key"
        key  = self._key_generator.next()
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

if __name__ == "__main__":
    unittest.main()
