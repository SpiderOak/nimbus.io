# -*- coding: utf-8 -*-
"""
test_data_reader.py

test the data reader process
"""
import os
import os.path
import shutil
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_final import RetrieveKeyFinal

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from diyapi_database_server.diyapi_database_server_main import \
        _database_cache

from unit_tests.archive_util import archive_coroutine
from unit_tests.retrieve_util import retrieve_coroutine

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

    def test_retrieve_small_content(self):
        """test retrieving content that fits in a single message"""
        avatar_id = 1001
        key  = _key_generator.next()
        version_number = 0
        segment_number = 5
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        archive_request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()
        data_writer_state = dict()
        data_reader_state = dict()
        database_state = {_database_cache : dict()}

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = ArchiveKeyEntire(
            archive_request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
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

        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            key,
            version_number,
            segment_number
        )
        
        retriever = retrieve_coroutine(
            self, data_reader_state, database_state, message
        )

        reply = retriever.next()
        self.assertEqual(reply.result, 0)

        self.assertEqual(reply.data_content, content_item)

    def test_retrieve_large_content(self):
        """test retrieving content that fits in a multiple messages"""
        segment_size = 120 * 1024
        chunk_count = 10
        total_size = int(1.2 * segment_size * chunk_count)
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = _key_generator.next()
        version_number = 0
        segment_number = 5
        sequence = 0
        archive_request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()
        data_writer_state = dict()
        data_reader_state = dict()
        database_state = {_database_cache : dict()}

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = ArchiveKeyStart(
            archive_request_id,
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
                archive_request_id,
                sequence,
                content_item
            )
            reply = archiver.send(message)
            self.assertEqual(reply.__class__, ArchiveKeyNextReply)
            self.assertEqual(reply.result, 0)
        
        sequence += 1
        message = ArchiveKeyFinal(
            archive_request_id,
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

        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            key,
            version_number,
            segment_number
        )
        
        retriever = retrieve_coroutine(
            self, data_reader_state, database_state, message
        )

        reply = retriever.next()
        self.assertEqual(reply.result, 0)
        segment_count = reply.segment_count

        # we have sequence 0, get sequence 1..N-1
        for sequence in range(1, segment_count-1):
            message = RetrieveKeyNext(request_id, sequence)
            reply = retriever.send(message)
            self.assertEqual(reply.result, 0)
            self.assertEqual(reply.data_content, test_data[sequence])

        # get the last segment
        sequence = segment_count - 1
        message = RetrieveKeyFinal(request_id, sequence)
        reply = retriever.send(message)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.data_content, test_data[sequence])

    def test_retrieve_large_content_short_last_segment(self):
        """
        test retrieving content that fits in a multiple messages
        with the last segment smaller than the others
        """
        segment_size = 120 * 1024
        short_size = 1024
        chunk_count = 10
        total_size = (segment_size * (chunk_count-1)) + short_size 
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count-1)]
        test_data.append(random_string(short_size))
        key  = _key_generator.next()
        version_number = 0
        segment_number = 5
        sequence = 0
        archive_request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()
        data_writer_state = dict()
        data_reader_state = dict()
        database_state = {_database_cache : dict()}

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = ArchiveKeyStart(
            archive_request_id,
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
                archive_request_id,
                sequence,
                content_item
            )
            reply = archiver.send(message)
            self.assertEqual(reply.__class__, ArchiveKeyNextReply)
            self.assertEqual(reply.result, 0)
        
        sequence += 1
        message = ArchiveKeyFinal(
            archive_request_id,
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

        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            key,
            version_number,
            segment_number
        )
        
        retriever = retrieve_coroutine(
            self, data_reader_state, database_state, message
        )

        reply = retriever.next()
        self.assertEqual(reply.result, 0)
        segment_count = reply.segment_count

        # we have sequence 0, get sequence 1..N-1
        for sequence in range(1, segment_count-1):
            message = RetrieveKeyNext(request_id, sequence)
            reply = retriever.send(message)
            self.assertEqual(reply.result, 0)
            self.assertEqual(reply.data_content, test_data[sequence])

        # get the last segment
        sequence = segment_count - 1
        message = RetrieveKeyFinal(request_id, sequence)
        reply = retriever.send(message)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.data_content, test_data[sequence])

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
        version_number = 0
        segment_number = 5
        sequence = 0
        archive_request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()
        data_writer_state = dict()
        data_reader_state = dict()
        database_state = {_database_cache : dict()}

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = ArchiveKeyStart(
            archive_request_id,
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
                archive_request_id,
                sequence,
                content_item
            )
            reply = archiver.send(message)
            self.assertEqual(reply.__class__, ArchiveKeyNextReply)
            self.assertEqual(reply.result, 0)
        
        sequence += 1
        message = ArchiveKeyFinal(
            archive_request_id,
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

        request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            key,
            version_number,
            segment_number
        )
        
        retriever = retrieve_coroutine(
            self, data_reader_state, database_state, message
        )

        reply = retriever.next()
        self.assertEqual(reply.result, 0)
        segment_count = reply.segment_count

        # we have sequence 0, get sequence 1..N-1
        for sequence in range(1, segment_count-1):
            message = RetrieveKeyNext(request_id, sequence)
            reply = retriever.send(message)
            self.assertEqual(reply.result, 0)
            self.assertEqual(reply.data_content, test_data[sequence])

        # get the last segment
        sequence = segment_count - 1
        message = RetrieveKeyFinal(request_id, sequence)
        reply = retriever.send(message)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.data_content, test_data[sequence])

if __name__ == "__main__":
    unittest.main()

