# -*- coding: utf-8 -*-
"""
test_data_writer.py

test the data writer process
"""
from base64 import b64encode
from datetime import timedelta
import hashlib
import os
import os.path
import shutil
import unittest
import uuid
import zlib

from tools.standard_logging import initialize_logging
from tools.data_definitions import create_timestamp, \
        create_priority, \
        nimbus_meta_prefix, \
        random_string
from tools.unified_id_factory import UnifiedIDFactory

from unit_tests.util import generate_key, \
        start_data_writer, \
        start_event_publisher, \
        poll_process, \
        terminate_process
from unit_tests.gevent_zeromq_util import send_request_and_get_reply

_log_path = "%s/test_data_writer.log" % (os.environ["NIMBUSIO_LOG_DIR"], )
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_cluster_name = os.environ["NIMBUSIO_CLUSTER_NAME"]
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_data_writer_address = os.environ["NIMBUSIO_DATA_WRITER_ADDRESS"]
_anti_entropy_address = os.environ["NIMBUSIO_DATA_WRITER_ANTI_ENTROPY_ADDRESS"]
_client_address = "tcp://127.0.0.1:8900"
_event_publisher_pull_address = \
    "ipc:///tmp/nimbusio-event-publisher-%s/socket" % (_local_node_name, )
_event_publisher_pub_address = "tcp://127.0.0.1:8800"


class TestDataWriter(unittest.TestCase):
    """test message handling in data writer"""

    def setUp(self):
        self.tearDown()
        self._key_generator = generate_key()

        self._event_publisher_process = start_event_publisher(
            _local_node_name, 
            _event_publisher_pull_address,
            _event_publisher_pub_address
        )
        poll_result = poll_process(self._event_publisher_process)
        self.assertEqual(poll_result, None)

    data_writer_type = os.environ["NIMBUSIO_DATA_WRITER"]
    if data_writer_type == "python":
        self._data_writer_process = start_data_writer(
            _cluster_name,
            _local_node_name, 
            _data_writer_address, 
            _anti_entropy_address,
            _event_publisher_pull_address,
            _event_publisher_pub_address,
            _repository_path,
            central_db_pw=os.environ["NIMBUSIO_CENTRAL_USER_PASSWORD"],
            node_db_pw=os.environ["NIMBUSIO_NODE_USER_PASSWORD"]
        )
        poll_result = poll_process(self._data_writer_process)
        self.assertEqual(poll_result, None)

    def tearDown(self):
        if hasattr(self, "_data_writer_process") \
        and self._data_writer_process is not None:
            terminate_process(self._data_writer_process)
            self._data_writer_process = None

        if hasattr(self, "_event_publisher_process") \
        and self._event_publisher_process is not None:
            terminate_process(self._event_publisher_process)
            self._event_publisher_process = None

    def xxxtest_archive_key_entire(self):
        """test archiving all data for a key in a single message"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        user_request_id = uuid.uuid1().hex
        collection_id = 1001
        key  = self._key_generator.next()
        archive_priority = create_priority()
        timestamp = create_timestamp()
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        unified_id_factory = UnifiedIDFactory(1)
        unified_id = unified_id_factory.next()

        message = {
            "message-type"      : "archive-key-entire",
            "priority"          : archive_priority,
            "user-request-id"   : user_request_id,
            "collection-id"     : collection_id,
            "key"               : key, 
            "unified-id"        : unified_id,
            "timestamp-repr"    : repr(timestamp),
            "conjoined-part"    : 0,
            "segment-num"       : segment_num,
            "segment-size"      : file_size,
            "zfec-padding-size" : 4,
            "segment-md5-digest": b64encode(file_md5.digest()),
            "segment-adler32"   : file_adler32,
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "source-node-name"  : _local_node_name,
            "handoff-node-name" : None,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["user-request-id"], user_request_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_archive_key_entire_with_meta(self):
        """
        test archiving a key in a single message, including meta data
        """
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        user_request_id = uuid.uuid1().hex
        collection_id = 1001
        key  = self._key_generator.next()
        archive_priority = create_priority()
        timestamp = create_timestamp()
        segment_num = 2

        meta_key = "".join([nimbus_meta_prefix, "test_key"])
        meta_value = "pork"

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        unified_id_factory = UnifiedIDFactory(1)
        unified_id = unified_id_factory.next()

        message = {
            "message-type"      : "archive-key-entire",
            "priority"          : archive_priority,
            "user-request-id"   : user_request_id,
            "collection-id"     : collection_id,
            "key"               : key, 
            "unified-id"        : unified_id,
            "timestamp-repr"    : repr(timestamp),
            "conjoined-part"    : 0,
             "segment-num"       : segment_num,
            "segment-size"      : file_size,
            "zfec-padding-size" : 4,
            "segment-adler32"   : file_adler32,
            "segment-md5-digest": b64encode(file_md5.digest()),
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "source-node-name"  : _local_node_name,
            "handoff-node-name" : None,
            meta_key            : meta_value
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["user-request-id"], user_request_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def xxxtest_large_archive(self):

        """
        test archiving a file that needs more than one message.
        For example, a 10 Mb file: each node would get 10 120kb 
        zefec shares.
        """
        slice_size = 1024 * 1024
        slice_count = 10
        total_size = slice_size * slice_count
        test_data = random_string(total_size)

        collection_id = 1001
        archive_priority = create_priority()
        timestamp = create_timestamp()
        key  = self._key_generator.next()
        segment_num = 4
        sequence_num = 0

        file_adler32 = zlib.adler32(test_data)
        file_md5 = hashlib.md5(test_data)

        slice_start = 0
        slice_end = slice_size

        segment_adler32 = zlib.adler32(test_data[slice_start:slice_end])
        segment_md5 = hashlib.md5(test_data[slice_start:slice_end])

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "segment-size"      : len(test_data[slice_start:slice_end]),
            "segment-adler32"   : segment_adler32,
            "segment-md5-digest": b64encode(segment_md5.digest()),
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

            segment_adler32 = zlib.adler32(test_data[slice_start:slice_end])
            segment_md5 = hashlib.md5(test_data[slice_start:slice_end])

            message_id = uuid.uuid1().hex
            message = {
                "message-type"      : "archive-key-next",
                "message-id"        : message_id,
                "priority"          : archive_priority,
                "collection-id"     : collection_id,
                "key"               : key, 
                "timestamp-repr"    : repr(timestamp),
                "segment-num"       : segment_num,
                "segment-size"      : len(test_data[slice_start:slice_end]),
                "segment-adler32"   : segment_adler32,
                "segment-md5-digest": b64encode(segment_md5.digest()),
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

        segment_adler32 = zlib.adler32(test_data[slice_start:slice_end])
        segment_md5 = hashlib.md5(test_data[slice_start:slice_end])

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-final",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(timestamp),
            "segment-num"       : segment_num,
            "segment-size"      : len(test_data[slice_start:slice_end]),
            "segment-adler32"   : segment_adler32,
            "segment-md5-digest": b64encode(segment_md5.digest()),
            "sequence-num"      : sequence_num,
            "file-size"         : total_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "handoff-node-name" : None,
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

    def _destroy(self, collection_id, key, timestamp, segment_num):
        message_id = uuid.uuid1().hex
        archive_priority = create_priority()
        message = {
            "message-type"      : "destroy-key",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
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

    def _purge(self, collection_id, key, timestamp, segment_num):
        message_id = uuid.uuid1().hex
        archive_priority = create_priority()
        message = {
            "message-type"      : "purge-key",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key,
            "timestamp-repr"    : repr(timestamp),
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

    def xxxtest_destroy_nonexistent_key(self):
        """test destroying a key that does not exist, with no complications"""
        collection_id = 1001
        key  = self._key_generator.next()
        segment_num = 4
        timestamp = create_timestamp()
        reply = self._destroy(collection_id, key, timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def xxxtest_simple_destroy(self):
        """test destroying a key that exists, with no complicatons"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        collection_id = 1001
        key  = self._key_generator.next()
        archive_priority = create_priority()
        archive_timestamp = create_timestamp()
        destroy_timestamp = archive_timestamp + timedelta(seconds=1)
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(archive_timestamp),
            "segment-num"       : segment_num,
            "segment-size"      : file_size,
            "segment-adler32"   : file_adler32,
            "segment-md5-digest": b64encode(file_md5.digest()),
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "handoff-node-name" : None,
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

        reply = self._destroy(
            collection_id, key, destroy_timestamp, segment_num
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def xxxtest_destroy_tombstone(self):
        """test destroying a key that has already been destroyed"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        collection_id = 1001
        key  = self._key_generator.next()
        archive_priority = create_priority()
        archive_timestamp = create_timestamp()
        destroy_1_timestamp = archive_timestamp + timedelta(seconds=1)
        destroy_2_timestamp = destroy_1_timestamp + timedelta(seconds=1)
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(archive_timestamp),
            "segment-num"       : segment_num,
            "segment-size"      : file_size,
            "segment-adler32"   : file_adler32,
            "segment-md5-digest": b64encode(file_md5.digest()),
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "handoff-node-name" : None,
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

        reply = self._destroy(
            collection_id, key, destroy_1_timestamp, segment_num
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])

        reply = self._destroy(
            collection_id, key, destroy_2_timestamp, segment_num
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def xxxtest_old_destroy(self):
        """
        test destroying a key that exists, but is newer than the destroy
        message
        """
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        collection_id = 1001
        key  = self._key_generator.next()
        archive_priority = create_priority()
        archive_timestamp = create_timestamp()
        destroy_timestamp = archive_timestamp - timedelta(seconds=1)
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(archive_timestamp),
            "segment-num"       : segment_num,
            "segment-size"      : file_size,
            "segment-adler32"   : file_adler32,
            "segment-md5-digest": b64encode(file_md5.digest()),
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "handoff-node-name" : None,
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

        reply = self._destroy(
            collection_id, key, destroy_timestamp, segment_num
        )
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def xxxtest_purge_nonexistent_key(self):
        """test purgeing a key that does not exist, with no complicatons"""
        collection_id = 1001
        key  = self._key_generator.next()
        segment_num = 4
        timestamp = create_timestamp()
        reply = self._purge(collection_id, key, timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply)

    def xxxtest_simple_purge(self):
        """test purging a key that exists, with no complicatons"""
        file_size = 10 * 64 * 1024
        content_item = random_string(file_size) 
        message_id = uuid.uuid1().hex
        collection_id = 1001
        key  = self._key_generator.next()
        archive_priority = create_priority()
        archive_timestamp = create_timestamp()
        segment_num = 2

        file_adler32 = zlib.adler32(content_item)
        file_md5 = hashlib.md5(content_item)

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "priority"          : archive_priority,
            "collection-id"     : collection_id,
            "key"               : key, 
            "timestamp-repr"    : repr(archive_timestamp),
            "segment-num"       : segment_num,
            "segment-size"      : file_size,
            "segment-adler32"   : file_adler32,
            "segment-md5-digest": b64encode(file_md5.digest()),
            "file-size"         : file_size,
            "file-adler32"      : file_adler32,
            "file-hash"         : b64encode(file_md5.digest()),
            "handoff-node-name" : None,
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

        reply = self._purge(collection_id, key, archive_timestamp, segment_num)
        self.assertEqual(reply["result"], "success", reply["error-message"])

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

