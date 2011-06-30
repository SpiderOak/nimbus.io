# -*- coding: utf-8 -*-
"""
test_data_writer.py

test the data writer process
"""
from base64 import b64encode
import hashlib
import os
import os.path
import shutil
import time
import unittest
import uuid
import zlib

from diyapi_tools.standard_logging import initialize_logging

from unit_tests.util import random_string, \
        generate_key, \
        start_data_writer, \
        poll_process, \
        terminate_process
from unit_tests.gevent_zeromq_util import send_request_and_get_reply

_log_path = "/var/log/pandora/test_data_writer.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
_local_node_name = "multi-node-01"
_data_writer_address = "tcp://127.0.0.1:8100"
_data_writer_pipeline_address = \
    "ipc:///tmp/spideroak-diyapi-data-writer-pipeline-%s/socket" % (
        _local_node_name,
    )
_client_address = "tcp://127.0.0.1:8900"

class TestDataWriter(unittest.TestCase):
    """test message handling in data writer"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

        self._data_writer_process = start_data_writer(
            _local_node_name, 
            _data_writer_address, 
            _data_writer_pipeline_address,
            _repository_path
        )
        poll_result = poll_process(self._data_writer_process)
        self.assertEqual(poll_result, None)

    def tearDown(self):
        if hasattr(self, "_data_writer_process") \
        and self._data_writer_process is not None:
            terminate_process(self._data_writer_process)
            self._data_wrter_process = None
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_archive_key_entire(self):
        """test archiving all data for a key in a single message"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        timestamp = time.time()
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : timestamp,
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

    def test_large_archive(self):

        """
        test archiving a file that needs more than one message.
        For example, a 10 Mb file: each node would get 10 120kb 
        zefec shares.
        """
        slice_size = 1024 * 1024
        slice_count = 10
        total_size = slice_size * slice_count
        test_data = random_string(total_size)

        avatar_id = 1001
        timestamp = time.time()
        key  = self._key_generator.next()
        segment_num = 4
        sequence_num = 0

        file_adler32 = zlib.adler32(test_data)
        file_md5 = hashlib.md5(test_data)

        slice_start = 0
        slice_end = slice_size

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : timestamp,
            "segment-num"       : segment_num,
            "sequence-num"      : sequence_num,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[slice_start:slice_end]
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-start-reply")
        self.assertEqual(reply["result"], "success")

        for _ in range(slice_count-2):
            sequence_num += 1
            slice_start += slice_size
            slice_end += slice_size
            message_id = uuid.uuid1().hex
            message = {
                "message-type"      : "archive-key-next",
                "avatar-id"         : avatar_id,
                "key"               : key, 
                "timestamp"         : timestamp,
                "segment-num"       : segment_num,
                "message-id"        : message_id,
                "sequence-num"      : sequence_num,
            }
            reply = send_request_and_get_reply(
                _local_node_name,
                _data_writer_address, 
                _local_node_name,
                _client_address,
                message, 
                data=test_data[slice_start:slice_end]
            )
            self.assertEqual(reply["message-id"], message_id)
            self.assertEqual(reply["message-type"], "archive-key-next-reply")
            self.assertEqual(reply["result"], "success")
        
        sequence_num += 1
        slice_start += slice_size
        slice_end += slice_size
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-final",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : timestamp,
            "segment-num"       : segment_num,
            "sequence-num"      : sequence_num,
            "file-size"         : total_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[-1]
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

    def _destroy(self, avatar_id, key, timestamp, segment_num):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "destroy-key",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "timestamp"         : timestamp,
            "segment-num"       : segment_num,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "destroy-key-reply")
        
        return reply

    def _purge(self, avatar_id, key, timestamp, segment_num):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "purge-key",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "timestamp"         : timestamp,
            "segment-num"       : segment_num,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "purge-key-reply")

        return reply

    def test_destroy_nonexistent_key(self):
        """test destroying a key that does not exist, with no complications"""
        avatar_id = 1001
        key  = self._key_generator.next()
        segment_num = 4
        timestamp = time.time()
        reply = self._destroy(avatar_id, key, timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_simple_destroy(self):
        """test destroying a key that exists, with no complicatons"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        destroy_timestamp = archive_timestamp + 1.0
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : archive_timestamp,
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        reply = self._destroy(avatar_id, key, destroy_timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_destroy_tombstone(self):
        """test destroying a key that has already been destroyed"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        destroy_1_timestamp = archive_timestamp + 1.0
        destroy_2_timestamp = destroy_1_timestamp + 1.0
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : archive_timestamp,
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        reply = self._destroy(avatar_id, key, destroy_1_timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

        reply = self._destroy(avatar_id, key, destroy_2_timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_old_destroy(self):
        """
        test destroying a key that exists, but is newer than the destroy
        message
        """
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        destroy_timestamp = archive_timestamp - 1.0
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : archive_timestamp,
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        reply = self._destroy(avatar_id, key, destroy_timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_purge_nonexistent_key(self):
        """test purgeing a key that does not exist, with no complicatons"""
        avatar_id = 1001
        key  = self._key_generator.next()
        segment_num = 4
        timestamp = time.time()
        reply = self._purge(avatar_id, key, timestamp, segment_num)
        self.assertEqual(reply["result"], "no-such-key", reply)

    def test_simple_purge(self):
        """test purging a key that exists, with no complicatons"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        archive_timestamp = time.time()
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "timestamp"         : archive_timestamp,
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-id"   : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        reply = self._purge(avatar_id, key, archive_timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

