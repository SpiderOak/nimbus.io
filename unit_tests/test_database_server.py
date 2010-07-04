# -*- coding: utf-8 -*-
"""
test_database_server.py

test database_server
"""
import logging
import os
import os.path
import shutil
import sys
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging
from diyapi_database_server import database_content
from messages.database_avatar_database_request import \
    DatabaseAvatarDatabaseRequest
from messages.database_avatar_database_reply import DatabaseAvatarDatabaseReply
from messages.database_avatar_list_request import DatabaseAvatarListRequest
from messages.database_avatar_list_reply import DatabaseAvatarListReply
from messages.database_consistency_check import DatabaseConsistencyCheck
from messages.database_consistency_check_reply import \
    DatabaseConsistencyCheckReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply
from messages.database_key_list import DatabaseKeyList
from messages.database_key_list_reply import DatabaseKeyListReply
from messages.database_key_destroy import DatabaseKeyDestroy
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply
from messages.database_key_purge import DatabaseKeyPurge
from messages.database_key_purge_reply import DatabaseKeyPurgeReply
from messages.database_listmatch import DatabaseListMatch
from messages.database_listmatch_reply import DatabaseListMatchReply

from unit_tests.util import generate_key, generate_database_content

_log_path = "/var/log/pandora/test_database_server.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")

os.environ["DIYAPI_REPOSITORY_PATH"] = _repository_path
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_insert, _handle_key_lookup, \
        _handle_key_list, _handle_key_destroy, _handle_key_purge, \
        _handle_listmatch, _handle_consistency_check, \
        _handle_avatar_database_request, \
        _handle_avatar_list_request

_reply_routing_header = "test_database_server"

def _create_database_state():
    return {
        _database_cache : dict(),
        "node-name"     : "node01",
    }


class TestDatabaseServer(unittest.TestCase):
    """test message handling in database server"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_repository_path)
        initialize_logging(_log_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def _insert_key(self, avatar_id, key, content):
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        message = DatabaseKeyInsert(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            key, 
            content
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_key_insert(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_key_insert_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)

        return reply

    def _lookup_key(self, avatar_id, key, version_number, segment_number):
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        message = DatabaseKeyLookup(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            key,
            version_number,
            segment_number
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_key_lookup(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_key_lookup_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)

        return reply

    def _list_key(self, avatar_id, key):
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        message = DatabaseKeyList(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            key
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_key_list(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_key_list_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)

        return reply

    def _destroy_key(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        message = DatabaseKeyDestroy(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_key_destroy(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_key_destroy_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)

        return reply

    def _purge_key(
        self, avatar_id, key, version_number, segment_number, timestamp
    ):
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        message = DatabaseKeyPurge(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_key_purge(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_key_purge_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)

        return reply

    def test_valid_key_insert(self):
        """test inserting data for a valid key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

    def test_duplicate_key_insert(self):
        """
        test inserting data for a valid key wiht two different segment numbers
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content1 = generate_database_content(segment_number=1)

        reply = self._insert_key(avatar_id, key, content1)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        content2 = generate_database_content(segment_number=2)

        reply = self._insert_key(avatar_id, key, content2)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

    def test_two_duplicate_keys_insert(self):
        """
        test inserting data for a valid key with three different segment numbers
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content1 = generate_database_content(segment_number=1)

        reply = self._insert_key(avatar_id, key, content1)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        content2 = generate_database_content(segment_number=2)

        reply = self._insert_key(avatar_id, key, content2)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        content3 = generate_database_content(segment_number=3)

        reply = self._insert_key(avatar_id, key, content3)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

    def test_key_insert_over_existing_key(self):
        """test inserting data for a valid key over some exsting data"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()
        original_size = content.total_size

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        new_content = content._replace(total_size=content.total_size+42)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, original_size)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        # now insert a duplicate key for a different segment number
        content2 = generate_database_content(segment_number=2)
        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        # now write over the first record
        new_content = content1._replace(total_size=content1.total_size+42)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, original_size)

    def test_key_insert_over_newer_existing_key(self):
        """
        test error condition where data timestamp is older than existing data
        """
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        new_content = content._replace(timestamp=content.timestamp-1.0)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(
            reply.result, DatabaseKeyInsertReply.error_invalid_duplicate
        )

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        # now insert a duplicate key for a different segment number
        content2 = generate_database_content(segment_number=2)
        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)
    
        # now attempt to write over the first record wiht an older timestamp
        new_content = content1._replace(timestamp=content1.timestamp-1.0)

        reply = self._insert_key(avatar_id, key, new_content)

        self.assertEqual(
            reply.result, DatabaseKeyInsertReply.error_invalid_duplicate
        )

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        reply = self._lookup_key(avatar_id, key, version_number, segment_number)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.database_content, content)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        content2 = generate_database_content(
            version_number=version_number,
            segment_number=2
        )

        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        reply = self._lookup_key(avatar_id, key, version_number, 1)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.database_content, content1)

        reply = self._lookup_key(avatar_id, key, version_number, 2)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.database_content, content2)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        reply = self._list_key(avatar_id, key)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(len(reply.content_list), 1)
        self.assertEqual(reply.content_list[0], content)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        content2 = generate_database_content(
            version_number=version_number,
            segment_number=2
        )

        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        reply = self._list_key(avatar_id, key)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(
            sorted(reply.content_list), sorted([content1, content2,])
        )

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
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        # simple destroy is where the destroy request is newer than
        # the database content
        destroy_timestamp = base_timestamp + 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, content.total_size)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number+1, base_timestamp+1.0
        )

        # we expect the request to succeed by creating a new tombstone
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

    def test_old_key_destroy(self):
        """test sending a destroy request older than the database content"""
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number=0
        segment_number = 4
        base_timestamp = time.time()
        content = generate_database_content(
            timestamp=base_timestamp,
            version_number=version_number,
            segment_number=segment_number
        )

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        destroy_timestamp = base_timestamp - 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply.result, DatabaseKeyDestroyReply.error_too_old)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        content2 = generate_database_content(
            timestamp=base_timestamp - 1.0,
            version_number=version_number,
            segment_number=2
        )

        reply = self._insert_key(avatar_id, key, content2)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        destroy_timestamp = base_timestamp - 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, 1, destroy_timestamp
        )

        self.assertEqual(reply.result, DatabaseKeyDestroyReply.error_too_old)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        # simple destroy is where the destroy request is newer than
        # the database content
        destroy_timestamp = base_timestamp + 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, content.total_size)

        # now let's destroy it again
        destroy_timestamp = destroy_timestamp + 1.0
        reply = self._destroy_key(
            avatar_id, key, version_number, segment_number, destroy_timestamp
        )

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.total_size, 0)

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
        self.assertEqual(reply.result, DatabaseKeyPurgeReply.error_no_such_key)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        # simple purge is where the purge request is newer than
        # the database content
        purge_timestamp = base_timestamp + 1.0
        reply = self._purge_key(
            avatar_id, key, version_number, segment_number, purge_timestamp
        )

        self.assertEqual(reply.result, 0, reply.error_message)

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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        reply = self._purge_key(
            avatar_id, key, version_number, segment_number+1, base_timestamp+1.0
        )

        # we expect the request to fail
        self.assertEqual(reply.result, DatabaseKeyPurgeReply.error_no_such_key)

    def test_listmatch_empty_database(self):
        """test listmach on an empty database"""
        avatar_id = 1001
        prefix = "xxx"
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"

        message = DatabaseListMatch(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            prefix
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_listmatch(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_listmatch_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.is_complete, True)
        self.assertEqual(reply.key_list, [])

    def test_listmatch_multiple_keys(self):
        """test listmach wiht multiple keys"""
        avatar_id = 1001
        prefix = "xxx"
        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"
        key_count = 1000
    
        key_list = ["%s-%05d" % (prefix, i, ) for i in xrange(key_count)]
        for key in key_list:

            content = generate_database_content()

            reply = self._insert_key(avatar_id, key, content)

            self.assertEqual(reply.result, 0, reply.error_message)
            self.assertEqual(reply.previous_size, 0)

        message = DatabaseListMatch(
            request_id,
            avatar_id,
            exchange,
            _reply_routing_header,
            prefix
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_listmatch(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_listmatch_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.is_complete, True)
        self.assertEqual(reply.key_list, key_list)

    def test_simple_consistency_check(self):
        """test consistency check on a simple key"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        request_id = uuid.uuid1().hex
        timestamp = time.time()
        exchange = "reply-exchange"

        message = DatabaseConsistencyCheck(
            request_id,
            avatar_id,
            timestamp,
            exchange,
            _reply_routing_header
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_consistency_check(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_consistence_check_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0, reply.error_message)

    def test_avatar_database_request(self):
        """test requesting database server to send us the database"""
        avatar_id = 1001
        key  = self._key_generator.next()
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        request_id = uuid.uuid1().hex
        dest_host = "127.0.0.1"
        dest_dir = os.path.join(_repository_path, "dest_dir")
        os.mkdir(dest_dir)

        exchange = "reply-exchange"

        message = DatabaseAvatarDatabaseRequest(
            request_id,
            avatar_id,
            dest_host,
            dest_dir,
            exchange,
            _reply_routing_header
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_avatar_database_request(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_avatar_database_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0, reply.error_message)
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

        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(reply.previous_size, 0)

        request_id = uuid.uuid1().hex
        exchange = "reply-exchange"

        message = DatabaseAvatarListRequest(
            request_id,
            exchange,
            _reply_routing_header
        )
        marshalled_message = message.marshall()

        state = _create_database_state()
        replies = _handle_avatar_list_request(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.database_avatar_list_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)
        reply_avatar_ids = list(reply.get())
        self.assertEqual(reply_avatar_ids[0], avatar_id)

if __name__ == "__main__":
    unittest.main()

