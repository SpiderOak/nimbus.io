# -*- coding: utf-8 -*-
"""
test_data_writer.py

test the data writer process
"""
from hashlib import md5
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid
from zlib import adler32

from tools.standard_logging import initialize_logging
from messages.archive_key_entire import ArchiveKeyEntire
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_data_writer")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_archive_key_entire

class TestDataWriter(unittest.TestCase):
    """test message handling in data writer"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_repository_path):
            shutil.rmtree(_repository_path)

    def test_archive_key_entire(self):
        """test archiving all data for a key in a single message"""
        original_content = random_string(64 * 1024) 
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        original_timestamp = time.time()
        original_segment_number = 3
        original_adler32 = adler32(original_content)
        original_md5 = md5(original_content).digest()
        message = ArchiveKeyEntire(
            original_request_id,
            original_avatar_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key, 
            original_timestamp,
            original_segment_number,
            original_adler32,
            original_md5,
            original_content
        )
        marshalled_message = message.marshall()

        state = dict()
        replies = _handle_archive_key_entire(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(reply_routing_key, routing_key)
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

if __name__ == "__main__":
    unittest.main()
