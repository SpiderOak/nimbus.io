# -*- coding: utf-8 -*-
"""
test_database_server.py

test AMQP Messages
"""
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid

from tools.standard_logging import initialize_logging
from diyapi_database_server import database_content
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

_log_path = "/var/log/pandora/test_database_server.log"
_test_dir = "/tmp"
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from diyapi_database_server.diyapi_database_server_main import \
        _handle_key_insert

class TestDatabaseServer(unittest.TestCase):
    """test message handling in database server"""

    def setUp(self):
        self.tearDown()
        os.mkdir(_repository_path)
        initialize_logging(_log_path)

    def tearDown(self):
        if os.path.exists(_repository_path):
            shutil.rmtree(_repository_path)

    def test_valid_key_insert(self):
        """test inserting data for a valid key"""
        avatar_id = 1001
        content = database_content.factory(
            timestamp=time.time(), 
            is_tombstone=False,  
            segment_number=1,  
            segment_size=42,  
            total_size=4200,  
            adler32=345, 
            md5="ffffffffffffffff" 
        )
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        routing_key = "reply.routing-key"
        key  = "abcdefghijk"
        message = DatabaseKeyInsert(
            request_id,
            avatar_id,
            exchange,
            routing_key,
            key, 
            content
        )
        marshalled_message = message.marshall()

        state = dict()
        replies = _handle_key_insert(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(reply_routing_key, routing_key)
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

if __name__ == "__main__":
    unittest.main()
