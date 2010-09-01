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

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools import amqp_connection

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["DIYAPI_REPOSITORY_PATH"] = _repository_path

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.destroy_key import DestroyKey
from messages.destroy_key_reply import DestroyKeyReply
from messages.database_key_destroy import DatabaseKeyDestroy
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply
from messages.purge_key import PurgeKey
from messages.purge_key_reply import PurgeKeyReply
from messages.database_key_purge import DatabaseKeyPurge
from messages.database_key_purge_reply import DatabaseKeyPurgeReply

from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_destroy, _handle_key_purge
from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_destroy_key, \
        _handle_key_destroy_reply, \
        _handle_purge_key, \
        _handle_key_purge_reply

from unit_tests.archive_util import archive_coroutine

_reply_routing_header = "test_archive"

class TestDataWriter(unittest.TestCase):
    """test message handling in data writer"""

    def setUp(self):
        logging.root.setLevel(logging.DEBUG)
        self.tearDown()
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_archive_key_entire(self):
        """test archiving all data for a key in a single message"""
        segment_size = 64 * 1024
        content_item = random_string(segment_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 2
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()
        data_writer_state = dict()
        database_state = {_database_cache : dict()}

        total_size = content_size - 42
        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content_item
        )

        archiver = archive_coroutine(
            self, data_writer_state, database_state, message
        )

        reply = archiver.next()

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
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        sequence = 0
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()
        data_writer_state = dict()
        database_state = {_database_cache : dict()}

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = ArchiveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            sequence,
            key, 
            version_number,
            segment_number,
            segment_size,
            test_data[0]
        )

        archiver = archive_coroutine(
            self, data_writer_state, database_state, message
        )   

        reply = archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyStartReply)
        self.assertEqual(reply.result, 0)

        for content_item in test_data[1:-1]:
            sequence += 1
            message = ArchiveKeyNext(
                request_id,
                sequence,
                content_item
            )
            reply = archiver.send(message)
            self.assertEqual(reply.__class__, ArchiveKeyNextReply)
            self.assertEqual(reply.result, 0)
        
        sequence += 1
        message = ArchiveKeyFinal(
            request_id,
            sequence,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            test_data[-1]
        )

        reply = archiver.send(message)

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

    def _destroy(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = DestroyKey(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
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

    def _purge(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = PurgeKey(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()

        data_writer_state = dict()
        replies = _handle_purge_key(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # after a successful write, we expect the data writer to send a
        # database_key_purge to the database server
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply_routing_key, DatabaseKeyPurge.routing_key)
        self.assertEqual(reply.__class__, DatabaseKeyPurge)
        self.assertEqual(reply.request_id, request_id)

        # hand off the reply to the database server
        marshalled_message = reply.marshall()
        database_state = {_database_cache : dict()}
        replies = _handle_key_purge(database_state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.request_id, request_id)

        # pass the database server reply back to data_writer
        # we should get a reply we can send to the web api 
        marshalled_message = reply.marshall()
        replies = _handle_key_purge_reply(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, PurgeKeyReply)

        return reply

    def test_destroy_nonexistent_key(self):
        """test destroying a key that does not exist, with no complicatons"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        timestamp = time.time()
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, timestamp
        )
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

    def test_simple_destroy(self):
        """test destroying a key that exists, with no complicatons"""
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        archive_timestamp = time.time()
        test_exchange = "test-exchange"

        total_size = content_size - 42
        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        data_writer_state = dict()
        database_state = {_database_cache : dict()}

        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            archive_timestamp,
            key, 
            version_number,
            segment_number,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content_item
        )

        archiver = archive_coroutine(
            self, data_writer_state, database_state, message
        )

        reply = archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        # the normal case is where the destroy mesage comes after the archive
        destroy_timestamp = archive_timestamp + 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, content_size)

    def test_destroy_tombstone(self):
        """test destroying a key that has already been destroyed"""
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        test_exchange = "test-exchange"

        total_size = content_size - 43
        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        archive_timestamp = time.time()
        data_writer_state = dict()
        database_state = {_database_cache : dict()}

        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            archive_timestamp,
            key, 
            version_number,
            segment_number,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content_item
        )

        archiver = archive_coroutine(
            self, data_writer_state, database_state, message
        )

        reply = archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        destroy_timestamp1 = archive_timestamp + 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp1
        )
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, content_size)

        # now send the same thing again
        destroy_timestamp2 = destroy_timestamp1 + 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp2
        )
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

    def test_old_destroy(self):
        """
        test destroying a key that exists, but is newer than the destroy
        message
        """
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        test_exchange = "test-exchange"

        total_size = 10 * content_size
        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        archive_timestamp = time.time()
        data_writer_state = dict()
        database_state = {_database_cache : dict()}

        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            archive_timestamp,
            key, 
            version_number,
            segment_number,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content_item
        )

        archiver = archive_coroutine(
            self, data_writer_state, database_state, message
        )

        reply = archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        # the destroy mesage is older than the archive
        destroy_timestamp = archive_timestamp - 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )
        self.assertEqual(
            reply.result, DestroyKeyReply.error_too_old, reply.error_message
        )

    def test_purge_nonexistent_key(self):
        """test purgeing a key that does not exist, with no complicatons"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        timestamp = time.time()
        reply = self._purge(
            avatar_id, key, version_number, segment_number, timestamp
        )
        self.assertEqual(reply.result, PurgeKeyReply.error_key_not_found)

    def test_simple_purge(self):
        """test purgeing a key that exists, with no complicatons"""
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        archive_timestamp = time.time()
        test_exchange = "test-exchange"

        total_size = content_size * 10
        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        data_writer_state = dict()
        database_state = {_database_cache : dict()}

        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            archive_timestamp,
            key, 
            version_number,
            segment_number,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content_item
        )

        archiver = archive_coroutine(
            self, data_writer_state, database_state, message
        )

        reply = archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        # the normal case is where the purge mesage comes after the archive
        purge_timestamp = archive_timestamp + 1.0
        reply = self._purge(
            avatar_id, key, version_number, segment_number, purge_timestamp
        )
        self.assertEqual(reply.result, 0, reply.error_message)

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

