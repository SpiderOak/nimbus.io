# -*- coding: utf-8 -*-
"""
test_data_writer.py

test the data writer process
"""
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid

from tools.standard_logging import initialize_logging
from tools import amqp_connection

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from messages.destroy_key import DestroyKey
from messages.destroy_key_reply import DestroyKeyReply
from messages.database_key_destroy import DatabaseKeyDestroy
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply

from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_destroy
from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_destroy_key, \
        _handle_key_destroy_reply

from unit_tests.archive_util import archive_small_content, \
        archive_large_content

_reply_routing_header = "test_archive"

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
        key  = self._key_generator.next()
        archive_small_content(self, avatar_id, key, content)

    def test_large_archive(self):
        """
        test archiving a file that needs more than one message.
        For example, a 10 Mb file: each node would get 10 120kb 
        zefec shares.
        """
        segment_size = 120 * 1024
        chunk_count = 10
        total_size = segment_size * chunk_count
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = self._key_generator.next()
        archive_large_content(
            self, avatar_id, key, segment_size, total_size, test_data
        )    

    def _destroy(self, avatar_id, key, timestamp):
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = DestroyKey(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            key, 
            timestamp
        )
        marshalled_message = message.marshall()

        data_writer_state = dict()
        replies = _handle_destroy_key(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # after a successful write, we expect the data writer to send a
        # database_key_destroy to the database server
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply_routing_key, DatabaseKeyDestroy.routing_key)
        self.assertEqual(reply.__class__, DatabaseKeyDestroy)
        self.assertEqual(reply.request_id, request_id)

        # hand off the reply to the database server
        marshalled_message = reply.marshall()
        database_state = {_database_cache : dict()}
        replies = _handle_key_destroy(database_state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.request_id, request_id)

        # pass the database server reply back to data_writer
        # we should get a reply we can send to the web api 
        marshalled_message = reply.marshall()
        replies = _handle_key_destroy_reply(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, DestroyKeyReply)

        return reply

    def test_destroy_nonexistent_key(self):
        """test destroying a key that does not exist, with no complicatons"""
        avatar_id = 1001
        key  = self._key_generator.next()
        timestamp = time.time()
        reply = self._destroy(avatar_id, key, timestamp)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

    def test_simple_destroy(self):
        """test destroying a key that exists, with no complicatons"""
        content_size = 64 * 1024
        content = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        archive_small_content(self, avatar_id, key, content, archive_timestamp)

        # the normal case is where the destroy mesage comes after the archive
        destroy_timestamp = archive_timestamp + 1.0
        reply = self._destroy(avatar_id, key, destroy_timestamp)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, content_size)

    def test_destroy_tombstone(self):
        """test destroying a key that has already been destroyed"""
        content_size = 64 * 1024
        content = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        archive_small_content(self, avatar_id, key, content, archive_timestamp)

        destroy_timestamp = archive_timestamp + 1.0
        reply = self._destroy(avatar_id, key, destroy_timestamp)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, content_size)

        # now send the same thing again
        destroy_timestamp = archive_timestamp + 1.0
        reply = self._destroy(avatar_id, key, destroy_timestamp)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

    def test_old_destroy(self):
        """
        test destroying a key that exists, but is newer than the destroy
        message
        """
        content_size = 64 * 1024
        content = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        archive_small_content(self, avatar_id, key, content, archive_timestamp)

        # the destroy mesage is older than the archive
        destroy_timestamp = archive_timestamp - 1.0
        reply = self._destroy(avatar_id, key, destroy_timestamp)
        self.assertEqual(
            reply.result, DestroyKeyReply.error_too_old, reply.error_message
        )

if __name__ == "__main__":
    unittest.main()
