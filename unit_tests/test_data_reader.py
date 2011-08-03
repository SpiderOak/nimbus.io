# -*- coding: utf-8 -*-
"""
test_data_reader.py

test the data reader process
"""
from base64 import b64encode
import hashlib
import os
import os.path
import shutil
import unittest
import uuid
import zlib

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.database_connection import get_node_local_connection
from diyapi_web_server.local_database_util import most_recent_timestamp_for_key
from diyapi_tools.data_definitions import create_timestamp

from unit_tests.util import random_string, \
        generate_key, \
        start_event_publisher, \
        start_data_writer, \
        start_data_reader, \
        poll_process, \
        terminate_process
from unit_tests.gevent_zeromq_util import send_request_and_get_reply_and_data, \
        send_request_and_get_reply

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
_cluster_name = "multi-node-cluster"
_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_data_writer_address = "tcp://127.0.0.1:8100"
_data_reader_address = "tcp://127.0.0.1:8200"
_client_address = "tcp://127.0.0.1:8900"
_event_publisher_pull_address = \
    "ipc:///tmp/spideroak-event-publisher-%s/socket" % (_local_node_name, )
_event_publisher_pub_address = "tcp://127.0.0.1:8800"

class TestDataReader(unittest.TestCase):
    """test message handling in data reader"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

        self._database_connection = get_node_local_connection()

        self._event_publisher_process = start_event_publisher(
            _local_node_name, 
            _event_publisher_pull_address,
            _event_publisher_pub_address
        )
        poll_result = poll_process(self._event_publisher_process)
        self.assertEqual(poll_result, None)

        self._data_writer_process = start_data_writer(
            _cluster_name,
            _local_node_name, 
            _data_writer_address,
            _event_publisher_pull_address,
            _repository_path
        )
        poll_result = poll_process(self._data_writer_process)
        self.assertEqual(poll_result, None)

        self._data_reader_process = start_data_reader(
            _local_node_name, 
            _data_reader_address,
            _repository_path
        )
        poll_result = poll_process(self._data_reader_process)
        self.assertEqual(poll_result, None)

    def tearDown(self):
        if hasattr(self, "_data_reader_process") \
        and self._data_reader_process is not None:
            terminate_process(self._data_reader_process)
            self._data_reader_process = None
        if hasattr(self, "_data_writer_process") \
        and self._data_writer_process is not None:
            terminate_process(self._data_writer_process)
            self._data_writer_process = None
        if hasattr(self, "_event_publisher_process") \
        and self._event_publisher_process is not None:
            terminate_process(self._event_publisher_process)
            self._event_publisher_process = None
        if hasattr(self, "_database_connection") \
        and self._database_connection is not None:
            self._database_connection.close()
            self._database_connection = None
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_retrieve_small_content(self):
        """test retrieving content that fits in a single message"""
        file_size = 10 * 64 * 1024
        file_content = random_string(file_size) 
        collection_id = 1001
        key  = self._key_generator.next()
        timestamp = create_timestamp()
        segment_num = 2

        file_adler32 = zlib.adler32(file_content)
        file_md5 = hashlib.md5(file_content)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "collection-id"         : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-name" : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=file_content
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        # get file info from the local database
        file_info = most_recent_timestamp_for_key(
            self._database_connection, collection_id, key
        )

        self.assertNotEqual(file_info, None)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-start",
            "message-id"        : message_id,
            "collection-id"         : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num
        }

        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message 
        )

        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-reply")
        self.assertEqual(reply["completed"], True)
        self.assertEqual(len(data), len(file_content))
        self.assertEqual(data, file_content)

    def test_retrieve_large_content(self):
        """test retrieving content that fits in a multiple messages"""
        slice_size = 1024 * 1024
        slice_count = 10
        total_size = slice_size * slice_count
        test_data = random_string(total_size)

        collection_id = 1001
        timestamp = create_timestamp()
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
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
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
                "collection-id"     : collection_id,
                "key"               : key, 
                "timestamp-repr"    : repr(timestamp),
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
        self.assertEqual(slice_end, total_size)
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-final",
            "message-id"        : message_id,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "sequence-num"      : sequence_num,
            "file-size"         : total_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "file-user-id"      : None,
            "file-group-id"     : None,
            "file-permissions"  : None,
            "handoff-node-name" : None,
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
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")

        # get file info from the local database
        file_info = most_recent_timestamp_for_key(
            self._database_connection, collection_id, key
        )

        self.assertNotEqual(file_info, None)

        retrieved_data_list = list()

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-start",
            "message-id"        : message_id,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num
        }

        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message 
        )
        
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-reply")
        self.assertEqual(reply["completed"], False)
        print "sequence-num =", reply["sequence-num"]

        retrieved_data_list.append(data)

        while True:
            message_id = uuid.uuid1().hex
            message = {
                "message-type"      : "retrieve-key-next",
                "message-id"        : message_id,
                "collection-id"     : collection_id,
                "key"               : key, 
                "timestamp-repr"    : repr(timestamp),
                "segment-num"       : segment_num
            }

            reply, data = send_request_and_get_reply_and_data(
                _local_node_name,
                _data_reader_address, 
                _local_node_name,
                _client_address,
                message 
            )
            
            self.assertEqual(reply["message-id"], message_id)
            self.assertEqual(reply["message-type"], "retrieve-key-reply")
            retrieved_data_list.append(data)
            print "sequence-num =", reply["sequence-num"]

            if reply["completed"]:
                break

        retrieved_data = "".join(retrieved_data_list)
        self.assertEqual(len(retrieved_data), len(test_data))
        self.assertEqual(retrieved_data, test_data)


if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

