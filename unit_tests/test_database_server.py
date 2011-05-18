# -*- coding: utf-8 -*-
"""
test_database_server.py

test database_server
"""
from base64 import b64decode
import os
import os.path
import shutil
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging
from diyapi_database_server import database_content

from unit_tests.util import generate_key, generate_database_content, \
        start_database_server, poll_process, terminate_process
from unit_tests.gevent_zeromq_util import send_request_and_get_reply, \
        send_request_and_get_reply_and_data

_log_path = "/var/log/pandora/test_database_server.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
_local_node_name = "node01"
_database_server_address = "tcp://127.0.0.1:8000"
_database_server_local_address = \
    "ipc:///tmp/spideroak-diyapi-database-server-%s/socket" % (
        _local_node_name,
    )    
_client_address = "tcp://127.0.0.1:8001"

class TestDatabaseServer(unittest.TestCase):
    """test message handling in database server"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()
        self._database_server_process = start_database_server(
            _local_node_name, 
            _database_server_address, 
            _database_server_local_address, 
            _repository_path
        )
        poll_result = poll_process(self._database_server_process)
        self.assertEqual(poll_result, None)

    def tearDown(self):
        if hasattr(self, "_database_server_process") \
        and self._database_server_process is not None:
            terminate_process(self._database_server_process)
            self._database_server_process = None
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def _insert_key(self, avatar_id, key, content):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-insert",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message, 
            data=database_content.marshall(content)
        )
        self.assertEqual(reply["message-id"], message_id)

        return reply

    def _lookup_key(self, avatar_id, key, version_number, segment_number):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-lookup",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
        }

        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        if reply["result"] == "success":
            reply_content, _ = database_content.unmarshall(data, 0)
        else:
            reply_content = None

        return reply, reply_content

    def _list_key(self, avatar_id, key):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-list",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
        }

        reply, data = send_request_and_get_reply_and_data(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        if reply["result"] != "success":
            data_list = []
        elif data is None:
            data_list = []
        elif type(data) != list:
            reply_content, _ = database_content.unmarshall(data, 0)
            data_list = [reply_content]
        else:
            data_list = list()
            for data_entry in data:
                reply_content, _ = database_content.unmarshall(data_entry, 0)
                data_list.append(reply_content)

        return reply, data_list

    def _destroy_key(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-destroy",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "timestamp"         : timestamp
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)

        return reply

    def _purge_key(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-purge",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "version-number"    : version_number,
            "segment-number"    : segment_number,
            "timestamp"         : timestamp
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)

        return reply

    def test_valid_key_insert(self):
        """test inserting data for a valid key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

    def test_duplicate_key_insert(self):
        """
        test inserting data for a valid key wiht two different segment numbers
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content1 = generate_database_content(segment_number=1)

        reply = self._insert_key(avatar_id, key, content1)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        content2 = generate_database_content(segment_number=2)

        reply = self._insert_key(avatar_id, key, content2)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

    def test_two_duplicate_keys_insert(self):
        """
        test inserting data for a valid key with three different segment numbers
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content1 = generate_database_content(segment_number=1)

        reply = self._insert_key(avatar_id, key, content1)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        content2 = generate_database_content(segment_number=2)

        reply = self._insert_key(avatar_id, key, content2)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        content3 = generate_database_content(segment_number=3)

        reply = self._insert_key(avatar_id, key, content3)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

    def test_key_insert_over_existing_key(self):
        """test inserting data for a valid key over some exsting data"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()
        original_size = content.total_size

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        new_content = content._replace(total_size=content.total_size+42)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], original_size)

    def test_key_insert_over_existing_key_with_duplicate(self):
        """
        test inserting data for a valid key over some exsting data
        when there is a duplicate key for a different segment number
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content1 = generate_database_content(segment_number=1)
        original_size = content1.total_size

        # insert a record
        reply = self._insert_key(avatar_id, key, content1)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        # now insert a duplicate key for a different segment number
        content2 = generate_database_content(segment_number=2)
        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        # now write over the first record
        new_content = content1._replace(total_size=content1.total_size+42)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], original_size)

    def test_key_insert_over_newer_existing_key(self):
        """
        test error condition where data timestamp is older than existing data
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        new_content = content._replace(timestamp=content.timestamp-1.0)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(reply["result"], "invalid-duplicate")

    def test_key_insert_over_newer_existing_key_with_duplicate(self):
        """
        test error condition where data timestamp is older than existing data
        where these is a duplicate key for a different segment number
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content1 = generate_database_content(segment_number=1)

        # insert a record
        reply = self._insert_key(avatar_id, key, content1)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        # now insert a duplicate key for a different segment number
        content2 = generate_database_content(segment_number=2)
        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)
    
        # now attempt to write over the first record wiht an older timestamp
        new_content = content1._replace(timestamp=content1.timestamp-1.0)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(reply["result"], "invalid-duplicate")

    def test_valid_key_lookup(self):
        """test retrieving data for a valid key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 1
        content = generate_database_content(
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        reply, reply_content = self._lookup_key(
            avatar_id, key, version_number, segment_number
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self._test_content(reply_content, content)

    def test_duplicate_key_lookup(self):
        """test retrieving data for a duplicate key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        content1 = generate_database_content(
            version_number=version_number,
            segment_number=1
        )

        reply = self._insert_key(avatar_id, key, content1)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        content2 = generate_database_content(
            version_number=version_number,
            segment_number=2
        )

        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        reply, reply_content = self._lookup_key(
            avatar_id, key, version_number, 1
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self._test_content(reply_content, content1)

        reply, reply_content = self._lookup_key(
            avatar_id, key, version_number, 2
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self._test_content(reply_content, content2)

    def test_simple_key_list(self):
        """test listing data for a key wiht no duplicagte entries"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 1
        content = generate_database_content(
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        reply, content_list = self._list_key(avatar_id, key)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(len(content_list), 1)
        self._test_content(content_list[0], content)

    def test_duplicate_key_list(self):
        """test listing data for a duplicate key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        content1 = generate_database_content(
            version_number=version_number,
            segment_number=1
        )

        reply = self._insert_key(avatar_id, key, content1)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        content2 = generate_database_content(
            version_number=version_number,
            segment_number=2
        )

        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        reply, content_list = self._list_key(avatar_id, key)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        for reply_content, original_content in zip(
            sorted(content_list), sorted([content1, content2,])
        ):
            self._test_content(reply_content, original_content)

    def test_key_destroy_on_nonexistent_key(self):
        """test destroying a key that does not exist"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        timestamp = time.time()

        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, timestamp
        )

        # we expect the request to succeed by creating a new tombstone
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], 0)

    def test_simple_key_destroy(self):
        """test destroying a key that exists"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 1
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        # simple destroy is where the destroy request is newer than
        # the database content
        destroy_timestamp = base_timestamp + 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], content.total_size)

    def test_key_destroy_on_nonexistent_duplicate(self):
        """
        test destroying a key for a segment number that does not exist
        when the key has another segment number that exists
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 1
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number+1, base_timestamp+1.0
        )

        # we expect the request to succeed by creating a new tombstone
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], 0)

    def test_old_key_destroy(self):
        """test sending a destroy request older than the database content"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        destroy_timestamp = base_timestamp - 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply["result"], "too-old")

    def test_old_key_destroy_with_duplicate(self):
        """
        test sending a destroy request older than the database content
        when the key has another segment number that exists
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        base_timestamp = time.time()
        version_number = 0
        content1 = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=1
        )

        reply = self._insert_key(avatar_id, key, content1)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        content2 = generate_database_content(
            timestamp=base_timestamp - 1.0,
            version_number=version_number,
            segment_number=2
        )

        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        destroy_timestamp = base_timestamp - 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, 1, destroy_timestamp
        )

        self.assertEqual(reply["result"], "too-old")

    def test_key_destroy_on_tombstone(self):
        """test destroying a key that was already destroyed"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number = segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        # simple destroy is where the destroy request is newer than
        # the database content
        destroy_timestamp = base_timestamp + 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], content.total_size)

        # now let's destroy it again
        destroy_timestamp = destroy_timestamp + 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["total-size"], 0)

    def test_key_purge_on_nonexistent_key(self):
        """test purgeing a key that does not exist"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 4
        timestamp = time.time()

        reply = self._purge_key(
            avatar_id, key, version_number, segment_number, timestamp
        )

        # we expect the request to fail
        self.assertEqual(reply["result"], "no-such-key")

    def test_simple_key_purge(self):
        """test purgeing a key that exists"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 1
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        # simple purge is where the purge request is newer than
        # the database content
        purge_timestamp = base_timestamp + 1.0
        reply = self._purge_key(
            avatar_id, key, version_number, segment_number, purge_timestamp
        )

        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_key_purge_on_nonexistent_duplicate(self):
        """
        test purgeing a key for a segment number that does not exist
        when the key has another segment number that exists
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 1
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        reply = self._purge_key(
            avatar_id, key, version_number, segment_number+1, base_timestamp+1.0
        )

        # we expect the request to fail
        self.assertEqual(reply["result"], "no-such-key")

    def test_listmatch_empty_database(self):
        """test listmach on an empty database"""
        avatar_id = 1001
        prefix = "xxx"
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "listmatch",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "prefix"            : prefix,
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["is-complete"], True)
        self.assertEqual(reply["key-list"], [])

    def test_listmatch_multiple_keys(self):
        """test listmach wiht multiple keys"""
        avatar_id = 1001
        prefix = "xxx"
        message_id = uuid.uuid1().hex
        key_count = 100
    
        key_list = ["%s-%05d" % (prefix, i, ) for i in xrange(key_count)]
        for key in key_list:

            content = generate_database_content()

            reply = self._insert_key(avatar_id, key, content)

            self.assertEqual(reply["result"], "success", reply["error-message"])
            self.assertEqual(reply["previous-size"], 0)

        message = {
            "message-type"      : "listmatch",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "prefix"            : prefix,
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["is-complete"], True)
        self.assertEqual(reply["key-list"], key_list)

    def test_simple_consistency_check(self):
        """test consistency check on a simple key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        timestamp = time.time()

        message = {
            "message-type"      : "consistency-check",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "timestamp"         : timestamp, 
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_avatar_database_request(self):
        """test requesting database server to send us the database"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        dest_host = "127.0.0.1"
        dest_dir = os.path.join(_repository_path, "dest_dir")
        os.mkdir(dest_dir)

        message = {
            "message-type"      : "avatar-database-request",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "dest-host"         : dest_host, 
            "dest-dir"          : dest_dir,
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])
        dest_list = os.listdir(dest_dir)
        # check that we got something, hoping database_server sent the right
        # thing
        self.assertEqual(len(dest_list), 1, dest_list)

    def test_avatar_list_request(self):
        """test requesting a list of avatar ids"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex

        message = {
            "message-type"      : "avatar-list-request",
            "message-id"        : message_id,
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)

        reply_avatar_ids = reply["avatar-id-list"]
        self.assertEqual(reply_avatar_ids[0], avatar_id)

    def test_stat(self):
        """test requesting stat on a key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply["result"], "success", reply["error-message"])
        self.assertEqual(reply["previous-size"], 0)

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "stat-request",
            "message-id"        : message_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "version-number"    : version_number,
        }

        reply = send_request_and_get_reply(
            _local_node_name,
            _database_server_address, 
            _local_node_name,
            _client_address,
            message
        )
        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])

        self.assertEqual(reply["message-id"], message_id)
        self.assertEqual(reply["result"], "success")
        self.assertEqual(reply["timestamp"], content.timestamp, reply)
        self.assertEqual(reply["total_size"], content.total_size)
        self.assertEqual(reply["file_adler32"], content.file_adler32)
        self.assertEqual(b64decode(reply["file_md5"]), content.file_md5)
        self.assertEqual(reply["userid"], content.userid)
        self.assertEqual(reply["groupid"], content.groupid)
        self.assertEqual(reply["permissions"], content.permissions)

    def _test_content(self, reply_content, original_content):
        reply_dict = reply_content._asdict()
        original_dict = original_content._asdict()
        for key in original_dict.keys():            
            self.assertEqual(reply_dict[key], original_dict[key])

if __name__ == "__main__":
    unittest.main()

