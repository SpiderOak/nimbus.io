# -*- coding: utf-8 -*-
"""
test_data_reader.py

test the data reader process
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
        start_data_reader, \
        poll_process, \
        terminate_process
from unit_tests.gevent_zeromq_util import send_request_and_get_reply_and_data, \
        send_request_and_get_reply

_log_path = "/var/log/pandora/test_data_reader.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
_local_node_name = "node01"
_database_server_address = "tcp://127.0.0.1:8000"
_database_server_local_address = \
    "ipc:///tmp/spideroak-diyapi-database-server-%s/socket" % (
        _local_node_name,
    )    
_data_writer_address = "tcp://127.0.0.1:8100"
_data_writer_pipeline_address = \
    "ipc:///tmp/spideroak-diyapi-data-writer-pipeline-%s/socket" % (
        _local_node_name,
    )
_data_reader_address = "tcp://127.0.0.1:8200"
_data_reader_pipeline_address = \
    "ipc:///tmp/spideroak-diyapi-data-reader-pipeline-%s/socket" % (
        _local_node_name,
    )
_client_address = "tcp://127.0.0.1:8900"

class TestDataReader(unittest.TestCase):
    """test message handling in data reader"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

        self._database_server_process = start_database_server(
            _local_node_name, 
            _database_server_address, 
            _database_server_local_address, 
            _repository_path
        )
        poll_result = poll_process(self._database_server_process)
        self.assertEqual(poll_result, None)

        self._data_writer_process = start_data_writer(
            _local_node_name, 
            _data_writer_address,
            _data_writer_pipeline_address,
            _database_server_address,
            _repository_path
        )
        poll_result = poll_process(self._data_writer_process)
        self.assertEqual(poll_result, None)

        self._data_reader_process = start_data_reader(
            _local_node_name, 
            _data_reader_address,
            _data_reader_pipeline_address,
            _database_server_address,
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
        if hasattr(self, "_database_server_process") \
        and self._database_server_process is not None:
            terminate_process(self._database_server_process)
            self._database_server_process = None
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_retrieve_small_content(self):
        """test retrieving content that fits in a single message"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 5
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        archive_message_id = uuid.uuid1().hex
        timestamp = time.time()

        total_size = content_size - 42
        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : archive_message_id,
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
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=content_item
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-start",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-start-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, content_item)

    def test_retrieve_large_content(self):
        """test retrieving content that fits in a multiple messages"""
        segment_size = 120 * 1024
        chunk_count = 10
        total_size = int(1.2 * segment_size * chunk_count)
        avatar_id = 1001
        test_data = [random_string(segment_size) for _ in range(chunk_count)]
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 5
        sequence = 0
        archive_message_id = uuid.uuid1().hex
        timestamp = time.time()

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : archive_message_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "sequence"          : sequence,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "segment-size"      : segment_size,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[sequence]
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-start-reply")
        self.assertEqual(reply["result"], "success")

        for content_item in test_data[1:-1]:
            sequence += 1
            message = {
                "message-type"      : "archive-key-next",
                "message-id"        : archive_message_id,
                "avatar-id"         : avatar_id,
                "key"               : key,
                "sequence"          : sequence,
            }
            reply = send_request_and_get_reply(
            _local_node_name,
                _data_writer_address, 
                _local_node_name,
                _client_address,
                message, 
                data=content_item
            )
            self.assertEqual(reply["message-id"], archive_message_id)
            self.assertEqual(reply["message-type"], "archive-key-next-reply")
            self.assertEqual(reply["result"], "success")
        
        sequence += 1
        message = {
            "message-type"      : "archive-key-final",
            "message-id"        : archive_message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[sequence]
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-start",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-start-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, test_data[0])

        segment_count = reply["segment-count"]

        # we have sequence 0, get sequence 1..N-1
        for sequence in range(1, segment_count-1):
            message = {
                "message-type"      : "retrieve-key-next",
                "message-id"        : message_id,
                "avatar-id"         : avatar_id,
                "key"               : key,
                "sequence"          : sequence,
            }
            reply, data = send_request_and_get_reply_and_data(
                _local_node_name,
                _data_reader_address, 
                _local_node_name,
                _client_address,
                message
            )
            self.assertEqual(reply["message-id"], message_id)
            self.assertEqual(reply["message-type"], "retrieve-key-next-reply")
            self.assertEqual(reply["result"], "success")
            self.assertEqual(data, test_data[sequence])

        # get the last segment
        sequence = segment_count - 1
        message = {
            "message-type"      : "retrieve-key-final",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence,
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, test_data[sequence])

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
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 5
        sequence = 0
        archive_message_id = uuid.uuid1().hex
        timestamp = time.time()

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : archive_message_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "sequence"          : sequence,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "segment-size"      : segment_size,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[sequence]
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-start-reply")
        self.assertEqual(reply["result"], "success")

        for content_item in test_data[1:-1]:
            sequence += 1
            message = {
                "message-type"      : "archive-key-next",
                "message-id"        : archive_message_id,
                "avatar-id"         : avatar_id,
                "key"               : key,
                "sequence"          : sequence,
            }
            reply = send_request_and_get_reply(
                _local_node_name,
                _data_writer_address, 
                _local_node_name,
                _client_address,
                message, 
                data=content_item
            )
            self.assertEqual(reply["message-id"], archive_message_id)
            self.assertEqual(reply["message-type"], "archive-key-next-reply")
            self.assertEqual(reply["result"], "success")
        
        sequence += 1
        message = {
            "message-type"      : "archive-key-final",
            "message-id"        : archive_message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[sequence]
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-start",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-start-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, test_data[0])

        segment_count = reply["segment-count"]

        # we have sequence 0, get sequence 1..N-1
        for sequence in range(1, segment_count-1):
            message = {
                "message-type"      : "retrieve-key-next",
                "message-id"        : message_id,
                "avatar-id"         : avatar_id,
                "key"               : key,
                "sequence"          : sequence,
            }
            reply, data = send_request_and_get_reply_and_data(
                _local_node_name,
                _data_reader_address, 
                _local_node_name,
                _client_address,
                message
            )
            self.assertEqual(reply["message-id"], message_id)
            self.assertEqual(reply["message-type"], "retrieve-key-next-reply")
            self.assertEqual(reply["result"], "success")
            self.assertEqual(data, test_data[sequence])

        # get the last segment
        sequence = segment_count - 1
        message = {
            "message-type"      : "retrieve-key-final",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence,
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, test_data[sequence])

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
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 5
        sequence = 0
        archive_message_id = uuid.uuid1().hex
        timestamp = time.time()

        file_adler32 = -42
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 32
        segment_md5 = "1111111111111111"

        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : archive_message_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp,
            "sequence"          : sequence,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "segment-size"      : segment_size,
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[sequence]
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-start-reply")
        self.assertEqual(reply["result"], "success")

        for content_item in test_data[1:-1]:
            sequence += 1
            message = {
                "message-type"      : "archive-key-next",
                "message-id"        : archive_message_id,
                "avatar-id"         : avatar_id,
                "key"               : key,
                "sequence"          : sequence,
            }
            reply = send_request_and_get_reply(
                _local_node_name,
                _data_writer_address, 
                _local_node_name,
                _client_address,
                message, 
                data=content_item
            )
            self.assertEqual(reply["message-id"], archive_message_id)
            self.assertEqual(reply["message-type"], "archive-key-next-reply")
            self.assertEqual(reply["result"], "success")
        
        sequence += 1
        message = {
            "message-type"      : "archive-key-final",
            "message-id"        : archive_message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : b64encode(file_md5),
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : b64encode(segment_md5),
        }
        reply = send_request_and_get_reply(
            _local_node_name,
            _data_writer_address, 
            _local_node_name,
            _client_address,
            message, 
            data=test_data[sequence]
        )
        self.assertEqual(reply["message-id"], archive_message_id)
        self.assertEqual(reply["message-type"], "archive-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-start",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "version-number"    : version_number,
            "segment-number"    : segment_number
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-start-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, test_data[0])

        segment_count = reply["segment-count"]

        # we have sequence 0, get sequence 1..N-1
        for sequence in range(1, segment_count-1):
            message = {
                "message-type"      : "retrieve-key-next",
                "message-id"        : message_id,
                "avatar-id"         : avatar_id,
                "key"               : key,
                "sequence"          : sequence,
            }
            reply, data = send_request_and_get_reply_and_data(
                _local_node_name,
                _data_reader_address, 
                _local_node_name,
                _client_address,
                message
            )
            self.assertEqual(reply["message-id"], message_id)
            self.assertEqual(reply["message-type"], "retrieve-key-next-reply")
            self.assertEqual(reply["result"], "success")
            self.assertEqual(data, test_data[sequence])

        # get the last segment
        sequence = segment_count - 1
        message = {
            "message-type"      : "retrieve-key-final",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key,
            "sequence"          : sequence,
        }
        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _data_reader_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["message-type"], "retrieve-key-final-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(data, test_data[sequence])

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

