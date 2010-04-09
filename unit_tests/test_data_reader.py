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

from diyapi_tools.standard_logging import initialize_logging

from unit_tests.util import random_string, generate_key

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from unit_tests.archive_util import archive_coroutine
from unit_tests.retrieve_util import retrieve_generator

_key_generator = generate_key()

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
        original_content = random_string(content_size) 

        archiver = archive_coroutine(
            self, 
            avatar_id, 
            key, 
            version_number,
            segment_number, 
            content_size,
            content_size
        )

        archiver.next()

        try:
            archiver.send((original_content, True, ))
        except StopIteration:
            pass

        retriever = retrieve_generator(
            self, avatar_id, key, version_number, segment_number
        )

        try:
            test_content = retriever.next()
        except StopIteration:
            pass

        self.assertEqual(test_content, original_content)

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

        archiver = archive_coroutine(
            self, 
            avatar_id, 
            key, 
            version_number,
            segment_number, 
            segment_size, 
            total_size 
        )

        archiver.next()

        for content_chunk in test_data[:-1]:
            archiver.send((content_chunk, False, ))

        try:
            archiver.send((test_data[-1], True, ))
        except StopIteration:
            pass

        retriever = retrieve_generator(
            self, avatar_id, key, version_number, segment_number
        )

        for test_chunk, retrieved_chunk in zip(test_data, retriever):
            self.assertEqual(test_chunk, retrieved_chunk)

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

        archiver = archive_coroutine(
            self, 
            avatar_id, 
            key, 
            version_number,
            segment_number, 
            segment_size, 
            total_size, 
        )    

        archiver.next()

        for content_chunk in test_data[:-1]:
            archiver.send((content_chunk, False, ))

        try:
            archiver.send((test_data[-1], True, ))
        except StopIteration:
            pass

        retriever = retrieve_generator(
            self, avatar_id, key, version_number, segment_number
        )

        for test_chunk, retrieved_chunk in zip(test_data, retriever):
            self.assertEqual(test_chunk, retrieved_chunk)

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

        archiver = archive_coroutine(
            self, 
            avatar_id, 
            key, 
            version_number,
            segment_number, 
            segment_size, 
            total_size, 
        )    

        archiver.next()

        for content_chunk in test_data[:-1]:
            archiver.send((content_chunk, False, ))

        try:
            archiver.send((test_data[-1], True, ))
        except StopIteration:
            pass

        retriever = retrieve_generator(
            self, avatar_id, key, version_number, segment_number
        )

        for test_chunk, retrieved_chunk in zip(test_data, retriever):
            self.assertEqual(test_chunk, retrieved_chunk)

if __name__ == "__main__":
    unittest.main()

