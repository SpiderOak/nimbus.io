# -*- coding: utf-8 -*-
"""
test_messages.py

test AMQP Messages
"""
from hashlib import md5
import logging
import time
import unittest
import uuid
from zlib import adler32

from unit_tests.util import generate_database_content

from diyapi_database_server import database_content
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_entire_reply import ArchiveKeyEntireReply
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.retrieve_key import RetrieveKey
from messages.retrieve_key_reply import RetrieveKeyReply

from unit_tests.util import random_string

class TestMessages(unittest.TestCase):
    """test AMQP Messages"""

    def test_database_key_insert(self):
        """test DatabaseKeyInsert"""
        original_content = generate_database_content()
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        message = DatabaseKeyInsert(
            original_request_id,
            original_avatar_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key, 
            original_content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyInsert.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.avatar_id, original_avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_key, 
            original_reply_routing_key
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, original_reply_exchange
        )
        self.assertEqual(unmarshalled_message.key, original_key)
        self.assertEqual(unmarshalled_message.content, original_content)

    def test_database_key_insert_reply_ok(self):
        """test DatabaseKeyInsertReply"""
        original_request_id = uuid.uuid1().hex
        original_result = 0
        original_previous_size = 42
        message = DatabaseKeyInsertReply(
            original_request_id,
            original_result,
            original_previous_size
        )
        marshaled_message = message.marshall()
        unmarshalled_message = DatabaseKeyInsertReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.result, original_result)
        self.assertEqual(
            unmarshalled_message.previous_size, original_previous_size
        )

    def test_database_key_lookup(self):
        """test DatabaseKeyLookup"""
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        message = DatabaseKeyLookup(
            original_request_id,
            original_avatar_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key, 
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyLookup.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.avatar_id, original_avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_key, 
            original_reply_routing_key
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, original_reply_exchange
        )
        self.assertEqual(unmarshalled_message.key, original_key)

    def test_database_key_lookup_reply_ok(self):
        """test DatabaseKeyLookupReply"""
        original_content = generate_database_content()
        marshalled_content = database_content.marshall(original_content)
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        message = DatabaseKeyLookupReply(
            original_request_id,
            0,
            marshalled_content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyLookupReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertTrue(unmarshalled_message.key_found)
        self.assertEqual(
            unmarshalled_message.unmarshalled_content, original_content
        )

    def test_archive_key_entire(self):
        """test ArchiveKeyEntire"""
        original_content = random_string(64 * 1024) 
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        original_timestamp = time.time()
        original_segment_number = 3
        original_adler32 = adler32(original_content)
        original_md5 = md5(original_content).digest()
        message = ArchiveKeyEntire(
            original_request_id,
            original_avatar_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key, 
            original_timestamp,
            original_segment_number,
            original_adler32,
            original_md5,
            original_content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyEntire.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.avatar_id, original_avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, original_reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_key, original_reply_routing_key
        )
        self.assertEqual(unmarshalled_message.key, original_key)
        self.assertEqual(unmarshalled_message.timestamp, original_timestamp)
        self.assertEqual(
            unmarshalled_message.segment_number, original_segment_number
        )
        self.assertEqual(unmarshalled_message.adler32, original_adler32)
        self.assertEqual(unmarshalled_message.md5, original_md5)
        self.assertEqual(unmarshalled_message.content, original_content)

    def test_archive_key_entire_reply_ok(self):
        """test ArchiveKeyEntireReply"""
        original_request_id = uuid.uuid1().hex
        original_result = 0
        original_previous_size = 42
        message = ArchiveKeyEntireReply(
            original_request_id,
            original_result,
            original_previous_size
        )
        marshaled_message = message.marshall()
        unmarshalled_message = ArchiveKeyEntireReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.result, original_result)
        self.assertEqual(
            unmarshalled_message.previous_size, original_previous_size
        )

    def test_archive_key_start(self):
        """test ArchiveKeyStart"""
        original_segment_size = 64 * 1024
        original_content = random_string(original_segment_size) 
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        original_timestamp = time.time()
        original_sequence = 0
        original_segment_number = 3

        message = ArchiveKeyStart(
            original_request_id,
            original_avatar_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key, 
            original_timestamp,
            original_sequence,
            original_segment_number,
            original_segment_size,
            original_content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyStart.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.avatar_id, original_avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, original_reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_key, original_reply_routing_key
        )
        self.assertEqual(unmarshalled_message.key, original_key)
        self.assertEqual(unmarshalled_message.timestamp, original_timestamp)
        self.assertEqual(unmarshalled_message.sequence, original_sequence)
        self.assertEqual(
            unmarshalled_message.segment_number, original_segment_number
        )
        self.assertEqual(
            unmarshalled_message.segment_size, original_segment_size
        )
        self.assertEqual(unmarshalled_message.data_content, original_content)

    def test_archive_key_start_reply_ok(self):
        """test ArchiveKeyStartReply"""
        original_request_id = uuid.uuid1().hex
        original_result = 0
        message = ArchiveKeyStartReply(
            original_request_id,
            original_result
        )
        marshaled_message = message.marshall()
        unmarshalled_message = ArchiveKeyStartReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.result, original_result)

    def test_archive_key_next(self):
        """test ArchiveKeyNext"""
        original_segment_size = 64 * 1024
        original_content = random_string(original_segment_size) 
        original_request_id = uuid.uuid1().hex
        original_sequence = 01

        message = ArchiveKeyNext(
            original_request_id,
            original_sequence,
            original_content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyNext.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.sequence, original_sequence)
        self.assertEqual(unmarshalled_message.data_content, original_content)

    def test_archive_key_next_reply_ok(self):
        """test ArchiveKeyNextReply"""
        original_request_id = uuid.uuid1().hex
        original_result = 0
        message = ArchiveKeyNextReply(
            original_request_id,
            original_result
        )
        marshaled_message = message.marshall()
        unmarshalled_message = ArchiveKeyNextReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.result, original_result)

    def test_retrieve_key(self):
        """test RetrieveKey"""
        original_request_id = uuid.uuid1().hex
        original_avatar_id = 1001
        original_reply_exchange = "reply-exchange"
        original_reply_routing_key = "reply.routing-key"
        original_key  = "abcdefghijk"
        message = RetrieveKey(
            original_request_id,
            original_avatar_id,
            original_reply_exchange,
            original_reply_routing_key,
            original_key 
        )
        marshalled_message = message.marshall()
        unmarshalled_message = RetrieveKey.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.avatar_id, original_avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, original_reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_key, original_reply_routing_key
        )
        self.assertEqual(unmarshalled_message.key, original_key)

    def test_retrieve_key_reply_ok(self):
        """test RetrieveKeyReply"""
        original_database_content = generate_database_content()
        original_data_content = random_string(64 * 1024) 
        original_request_id = uuid.uuid1().hex
        original_result = 0
        message = RetrieveKeyReply(
            original_request_id,
            original_result,
            original_database_content,
            original_data_content
        )
        marshaled_message = message.marshall()
        unmarshalled_message = RetrieveKeyReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, original_request_id)
        self.assertEqual(unmarshalled_message.result, original_result)
        self.assertEqual(
            unmarshalled_message.database_content, original_database_content
        )
        self.assertEqual(
            unmarshalled_message.data_content, original_data_content
        )

if __name__ == "__main__":
    unittest.main()
