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

from unit_tests.archive_util import archive_small_content, \
        archive_large_content

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

    def test_destroy_nonexistent_key(self):
        """test destroying a key that does not exists, wiht no complicatons"""
        pass

    def test_simple_destroy(self):
        """test destroying a key that exists, wiht no complicatons"""
        pass

if __name__ == "__main__":
    unittest.main()
