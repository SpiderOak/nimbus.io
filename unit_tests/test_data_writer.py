 # -*- coding: utf-8 -*-
"""
test_data_writer.py

test the data writer process
"""
from base64 import b64encode
import os
import os.path
import shutil
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging

from unit_tests.util import random_string, \
        generate_key, \
        start_database_server,\
        start_data_writer, \
        poll_process, \
        terminate_process
from unit_tests.zeromq_util import send_request_and_get_reply

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
_local_node_name = "node01"
_database_server_address = "tcp://127.0.0.1:8000"
_data_writer_address = "tcp://127.0.0.1:8100"

class TestDataWriter(unittest.TestCase):
    """test message handling in data writer"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

        self._database_server_process = start_database_server(
            _local_node_name, _database_server_address, _repository_path
        )
        poll_result = poll_process(self._database_server_process)
        self.assertEqual(poll_result, None)

        self._data_writer_process = start_data_writer(
            _local_node_name, 
            _data_writer_address, 
            _database_server_address,
            _repository_path
        )
        poll_result = poll_process(self._data_writer_process)
        self.assertEqual(poll_result, None)

    def tearDown(self):
        if hasattr(self, "_data_writer_process") \
        and self._data_writer_process is not None:
            terminate_process(self._data_writer_process)
            self._data_wrter_process = None
        if hasattr(self, "_database_server_process") \
        and self._database_server_process is not None:
            terminate_process(self._database_server_process)
            self._database_server_process = None
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
        timestamp = time.time()

        total_size = 10 * 64 * 1024
        file_adler32 = -42
        file_md5 = '\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1'
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-entire",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=content_item
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

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
        timestamp = time.time()

        file_adler32 = -42
        file_md5 = '\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1'
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-start",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "sequence"          : sequence,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "segment-size"      : segment_size,
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=test_data[0]
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-start-reply")
        self.assertEqual(reply["result"], "success")

        for content_item in test_data[1:-1]:
            sequence += 1
            message = {
                "message-type"      : "archive-key-next",
                "request-id"        : request_id,
                "sequence"          : sequence,
            }
            reply = send_request_and_get_reply(
                _data_writer_address, message, data=content_item
            )
            self.assertEqual(reply["request-id"], request_id)
            self.assertEqual(reply["message-type"], "archive-key-next-reply")
            self.assertEqual(reply["result"], "success")
        
        sequence += 1
        message = {
            "message-type"      : "archive-key-final",
            "request-id"        : request_id,
            "sequence"          : sequence,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=test_data[-1]
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

    def _destroy(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        request_id = uuid.uuid1().hex
        message = {
            "message-type"      : "destroy-key",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number,
        }
        reply = send_request_and_get_reply(_data_writer_address, message)
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "destroy-key-reply")
        
        return reply

    def _purge(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        request_id = uuid.uuid1().hex
        message = {
            "message-type"      : "purge-key",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number,
        }
        reply = send_request_and_get_reply(_data_writer_address, message)
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "purge-key-reply")

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
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], 0)

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

        total_size = content_size - 42
        file_adler32 = -42
        file_md5 = '\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1'
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-entire",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : archive_timestamp,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=content_item
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        # the normal case is where the destroy mesage comes after the archive
        destroy_timestamp = archive_timestamp + 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], total_size)

    def test_destroy_tombstone(self):
        """test destroying a key that has already been destroyed"""
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4

        total_size = content_size - 43
        file_adler32 = -42
        file_md5 = '\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1'
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        archive_timestamp = time.time()

        message = {
            "message-type"      : "archive-key-entire",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : archive_timestamp,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=content_item
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        destroy_timestamp1 = archive_timestamp + 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp1
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], total_size)

        # now send the same thing again
        destroy_timestamp2 = destroy_timestamp1 + 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp2
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], 0)

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

        total_size = 10 * content_size
        file_adler32 = -42
        file_md5 = '\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1'
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        archive_timestamp = time.time()

        message = {
            "message-type"      : "archive-key-entire",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : archive_timestamp,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=content_item
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        # the destroy mesage is older than the archive
        destroy_timestamp = archive_timestamp - 1.0
        reply = self._destroy(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )
        self.assertEqual(reply["result"], "too-old", reply["error-message"])

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
        self.assertEqual(reply["result"], "no-such-key", reply)

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

        total_size = content_size * 10
        file_adler32 = -42
        file_md5 = '\x936\xeb\xf2P\x87\xd9\x1c\x81\x8e\xe6\xe9\xec)\xf8\xc1'
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-entire",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : archive_timestamp,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _data_writer_address, message, data=content_item
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        # the normal case is where the purge mesage comes after the archive
        purge_timestamp = archive_timestamp + 1.0
        reply = self._purge(
            avatar_id, key, version_number, segment_number, purge_timestamp
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

