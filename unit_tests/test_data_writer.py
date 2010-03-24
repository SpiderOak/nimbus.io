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
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_data_writer")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_archive_key_entire, _handle_key_insert_reply, \
        _handle_archive_key_start, \
        _handle_archive_key_next, \
        _handle_archive_key_final
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
        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

    def test_large_archive(self):
        """
        test archiving a file that needs more than one message.
        For example, a 10 Mb file: each node would get 10 120kb 
        zefec shares.
        """
        segment_size = 120 * 1024
        chunk_count = 10
        total_size = segment_size * chunk_count
        test_data = [random_string(segment_size) for _ in range(chunk_count)]

        request_id = uuid.uuid1().hex
        avatar_id = 1001
        test_exchange = "reply-exchange"
        test_routing_key = "reply.routing-key"
        key  = self._key_generator.next()
        timestamp = time.time()
        segment_number = 3

        # the adler32 and md5 hashes should be of the original pre-zefec
        # data segment. We don't have that so we make something up.
        adler32 = -42
        md5 = "ffffffffffffffff"

        sequence = 0

        message = ArchiveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            test_routing_key,
            key, 
            timestamp,
            sequence,
            segment_number,
            segment_size,
            test_data[0]
        )
        marshalled_message = message.marshall()

        data_writer_state = dict()
        replies = _handle_archive_key_start(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we should get a successful reply 
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, ArchiveKeyStartReply)
        self.assertEqual(reply.result, 0)

        # do the interior content
        for test_data_content in test_data[1:-1]:
            sequence += 1

            message = ArchiveKeyNext(
                request_id,
                sequence,
                test_data_content
            )
            marshalled_message = message.marshall()

            replies = _handle_archive_key_next(
                data_writer_state, marshalled_message
            )
            self.assertEqual(len(replies), 1)

            # we should get a successful reply 
            [(reply_exchange, reply_routing_key, reply, ), ] = replies
            self.assertEqual(reply.__class__, ArchiveKeyNextReply)
            self.assertEqual(reply.result, 0)

        # send the last one
        sequence += 1

        message = ArchiveKeyFinal(
            request_id,
            sequence,
            total_size,
            adler32,
            md5,
            test_data[-1]
        )
        marshalled_message = message.marshall()

        replies = _handle_archive_key_final(
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
        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

if __name__ == "__main__":
    unittest.main()
