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

from messages.anti_entropy_audit_request import AntiEntropyAuditRequest
from messages.anti_entropy_audit_reply import AntiEntropyAuditReply
from diyapi_database_server import database_content
from messages.database_avatar_database_request import \
    DatabaseAvatarDatabaseRequest
from messages.database_avatar_database_reply import \
    DatabaseAvatarDatabaseReply
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
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.destroy_key import DestroyKey
from messages.destroy_key_reply import DestroyKeyReply
from messages.purge_key import PurgeKey
from messages.purge_key_reply import PurgeKeyReply
from messages.process_status import ProcessStatus
from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply
from messages.space_accounting_detail import SpaceAccountingDetail
from messages.rebuild_request import RebuildRequest
from messages.rebuild_reply import RebuildReply
from messages.stat import Stat
from messages.stat_reply import StatReply
from messages.space_usage import SpaceUsage
from messages.space_usage_reply import SpaceUsageReply

from unit_tests.util import random_string

class TestMessages(unittest.TestCase):
    """test AMQP Messages"""

    def test_anti_entropy_audit_request(self):
        """test AntiEntropyAuditRequest"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        message = AntiEntropyAuditRequest(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = AntiEntropyAuditRequest.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_anti_entropy_audit_reply(self):
        """test AntiEntropyAuditReply"""
        request_id = uuid.uuid1().hex
        result = 0
        message = AntiEntropyAuditReply(request_id, result)
        marshalled_message = message.marshall()
        unmarshalled_message = AntiEntropyAuditReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_database_avatar_database_request(self):
        """test DatabaseAvatarDatabaseRequest"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        dest_host = "localhost"
        dest_dir = "/var/pandora/xxxx"
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        message = DatabaseAvatarDatabaseRequest(
            request_id,
            avatar_id,
            dest_host,
            dest_dir,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseAvatarDatabaseRequest.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(unmarshalled_message.dest_host, dest_host)
        self.assertEqual(unmarshalled_message.dest_dir, dest_dir)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_database_avatar_database_reply(self):
        """test DatabaseAvatarDatabaseReply"""
        request_id = uuid.uuid1().hex
        node_name = "mpde-1"
        result = 0
        message = DatabaseAvatarDatabaseReply(request_id, node_name, result)
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseAvatarDatabaseReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.node_name, node_name)
        self.assertEqual(unmarshalled_message.result, result)

    def test_database_avatar_list_request(self):
        """test DatabaseAvatarListRequest"""
        request_id = uuid.uuid1().hex
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        message = DatabaseAvatarListRequest(
            request_id,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseAvatarListRequest.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_database_avatar_list_reply(self):
        """test DatabaseAvatarListReply"""
        request_id = uuid.uuid1().hex
        avatar_id_list = [1001, 1002, 1003, ]
        message = DatabaseAvatarListReply(request_id)
        message.put(avatar_id_list)
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseAvatarListReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        for avatar_id, reply_avatar_id in zip(
            avatar_id_list, unmarshalled_message.get()
        ):
            self.assertEqual(avatar_id, reply_avatar_id)

    def test_database_consistency_check(self):
        """test DatabaseConsistencyCheck"""
        content = generate_database_content()
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        timestamp = time.time()
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        message = DatabaseConsistencyCheck(
            request_id,
            avatar_id,
            timestamp,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseConsistencyCheck.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_database_consistency_check_reply_ok(self):
        """test DatabaseConsistencyCheckReply"""
        request_id = uuid.uuid1().hex
        node_name = "node01"
        result = 0
        hash = md5("test").digest()
        message = DatabaseConsistencyCheckReply(
            request_id,
            node_name,
            result,
            hash
        )
        marshaled_message = message.marshall()
        unmarshalled_message = DatabaseConsistencyCheckReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.node_name, node_name)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(unmarshalled_message.hash, hash)

    def test_database_key_insert(self):
        """test DatabaseKeyInsert"""
        content = generate_database_content()
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        key  = "abcdefghijk"
        message = DatabaseKeyInsert(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            key, 
            content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyInsert.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.database_content, content
        )

    def test_database_key_insert_reply_ok(self):
        """test DatabaseKeyInsertReply"""
        request_id = uuid.uuid1().hex
        result = 0
        previous_size = 42
        message = DatabaseKeyInsertReply(
            request_id,
            result,
            previous_size
        )
        marshaled_message = message.marshall()
        unmarshalled_message = DatabaseKeyInsertReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(
            unmarshalled_message.previous_size, previous_size
        )

    def test_database_key_lookup(self):
        """test DatabaseKeyLookup"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        key  = "abcdefghijk"
        version_number = 0
        segment_number = 1
        message = DatabaseKeyLookup(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            key, 
            version_number,
            segment_number
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyLookup.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )

    def test_database_key_lookup_reply_ok(self):
        """test DatabaseKeyLookupReply"""
        content = generate_database_content()
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        message = DatabaseKeyLookupReply(
            request_id,
            0,
            content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyLookupReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(
            unmarshalled_message.database_content, content
        )

    def test_database_key_list(self):
        """test DatabaseKeyList"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        key  = "abcdefghijk"
        message = DatabaseKeyList(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            key 
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyList.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(unmarshalled_message.key, key)

    def test_database_key_list_reply_ok(self):
        """test DatabaseKeyListReply"""
        content_list = [
            generate_database_content(segment_number=n) for n in range(3)
        ]
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        message = DatabaseKeyListReply(
            request_id,
            0,
            content_list
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyListReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(
            unmarshalled_message.content_list, content_list
        )

    def test_database_key_destroy(self):
        """test DatabaseKeyDestroy"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        key  = "abcdefghijk"
        version_number = 0
        segment_number = 4
        message = DatabaseKeyDestroy(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            key,
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyDestroy.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.timestamp, timestamp
        )
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )

    def test_database_key_destroy_reply_ok(self):
        """test DatabaseKeyDestroyReply"""
        request_id = uuid.uuid1().hex
        result = 0
        total_size = 42
        message = DatabaseKeyDestroyReply(
            request_id,
            result,
            total_size
        )
        marshaled_message = message.marshall()
        unmarshalled_message = DatabaseKeyDestroyReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(
            unmarshalled_message.total_size, total_size
        )

    def test_database_key_purge(self):
        """test DatabaseKeyPurge"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        key  = "abcdefghijk"
        version_number = 0
        segment_number = 4
        message = DatabaseKeyPurge(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            key,
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseKeyPurge.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.timestamp, timestamp
        )
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )

    def test_database_key_purge_reply_ok(self):
        """test DatabaseKeyPurgeReply"""
        request_id = uuid.uuid1().hex
        result = 0
        message = DatabaseKeyPurgeReply(
            request_id,
            result
        )
        marshaled_message = message.marshall()
        unmarshalled_message = DatabaseKeyPurgeReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_database_listmatch(self):
        """test DatabaseListMatch"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        prefix  = "abcdefghijk"
        message = DatabaseListMatch(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            prefix 
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseListMatch.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(unmarshalled_message.prefix, prefix)

    def test_database_listmatch_reply_ok(self):
        """test DatabaseListMatchReply"""
        request_id = uuid.uuid1().hex
        result = 0
        is_complete = True
        key_list = [str(x) for x in range(1000)]
        message = DatabaseListMatchReply(
            request_id,
            result,
            is_complete,
            key_list
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DatabaseListMatchReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(unmarshalled_message.is_complete, is_complete)
        self.assertEqual(unmarshalled_message.key_list, key_list)

    def test_archive_key_entire(self):
        """test ArchiveKeyEntire"""
        content = random_string(64 * 1024) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        key  = "abcdefghijk"
        version_number = 0
        segment_number = 3
        file_adler32 = adler32(content)
        file_md5 = md5(content).digest()
        segment_adler32 = 42
        segment_md5 = "ffffffffffffffff"
        message = ArchiveKeyEntire(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyEntire.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, reply_routing_header
        )
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )
        self.assertEqual(
            unmarshalled_message.file_adler32, file_adler32
        )
        self.assertEqual(
            unmarshalled_message.file_md5, file_md5
        )
        self.assertEqual(
            unmarshalled_message.segment_adler32, segment_adler32
        )
        self.assertEqual(
            unmarshalled_message.segment_md5, segment_md5
        )
        self.assertEqual(unmarshalled_message.content, content)

    def test_archive_key_start(self):
        """test ArchiveKeyStart"""
        segment_size = 64 * 1024
        content = random_string(segment_size) 
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        sequence = 0
        key  = "abcdefghijk"
        version_number = 1
        segment_number = 3

        message = ArchiveKeyStart(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            sequence,
            key, 
            version_number,
            segment_number,
            segment_size,
            content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyStart.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, reply_routing_header
        )
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.sequence, sequence)
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )
        self.assertEqual(
            unmarshalled_message.segment_size, segment_size
        )
        self.assertEqual(unmarshalled_message.data_content, content)

    def test_archive_key_start_reply_ok(self):
        """test ArchiveKeyStartReply"""
        request_id = uuid.uuid1().hex
        result = 0
        message = ArchiveKeyStartReply(
            request_id,
            result
        )
        marshaled_message = message.marshall()
        unmarshalled_message = ArchiveKeyStartReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_archive_key_next(self):
        """test ArchiveKeyNext"""
        segment_size = 64 * 1024
        content = random_string(segment_size) 
        request_id = uuid.uuid1().hex
        sequence = 01

        message = ArchiveKeyNext(
            request_id,
            sequence,
            content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyNext.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.sequence, sequence)
        self.assertEqual(unmarshalled_message.data_content, content)

    def test_archive_key_next_reply_ok(self):
        """test ArchiveKeyNextReply"""
        request_id = uuid.uuid1().hex
        result = 0
        message = ArchiveKeyNextReply(
            request_id,
            result
        )
        marshaled_message = message.marshall()
        unmarshalled_message = ArchiveKeyNextReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_archive_key_final(self):
        """test ArchiveKeyFinal"""
        content = random_string(64 * 1024) 
        request_id = uuid.uuid1().hex
        sequence = 3
        total_size = 42L
        file_adler32 = 10
        file_md5 = "ffffffffffffffff"
        segment_adler32 = 100
        segment_md5 = "1111111111111111"
        message = ArchiveKeyFinal(
            request_id,
            sequence,
            total_size,
            file_adler32,
            file_md5,
            segment_adler32,
            segment_md5,
            content
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ArchiveKeyFinal.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.sequence, sequence)
        self.assertEqual(unmarshalled_message.total_size, total_size)
        self.assertEqual(
            unmarshalled_message.file_adler32, file_adler32
        )
        self.assertEqual(
            unmarshalled_message.file_md5, file_md5
        )
        self.assertEqual(
            unmarshalled_message.segment_adler32, segment_adler32
        )
        self.assertEqual(
            unmarshalled_message.segment_md5, segment_md5
        )
        self.assertEqual(unmarshalled_message.data_content, content)

    def test_archive_key_final_reply_ok(self):
        """test ArchiveKeyFinalReply"""
        request_id = uuid.uuid1().hex
        result = 0
        previous_size = 42
        message = ArchiveKeyFinalReply(
            request_id,
            result,
            previous_size
        )
        marshaled_message = message.marshall()
        unmarshalled_message = ArchiveKeyFinalReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(
            unmarshalled_message.previous_size, previous_size
        )

    def test_retrieve_key_start(self):
        """test RetrieveKeyStart"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        key  = "abcdefghijk"
        version_number = 0
        segment_number = 5
        message = RetrieveKeyStart(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            key,
            version_number,
            segment_number
        )
        marshalled_message = message.marshall()
        unmarshalled_message = RetrieveKeyStart.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, reply_routing_header
        )
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )

    def test_retrieve_key_start_reply_ok(self):
        """test RetrieveKeyStartReply"""
        database_content = generate_database_content()
        data_content = random_string(64 * 1024) 
        request_id = uuid.uuid1().hex
        result = 0
        message = RetrieveKeyStartReply(
            request_id,
            result,
            database_content.timestamp,
            database_content.is_tombstone,
            database_content.version_number,
            database_content.segment_number,
            database_content.segment_count,
            database_content.segment_size,
            database_content.total_size,
            database_content.file_adler32,
            database_content.file_md5,
            database_content.segment_adler32,
            database_content.segment_md5,
            data_content
        )
        marshaled_message = message.marshall()
        unmarshalled_message = RetrieveKeyStartReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(
            unmarshalled_message.timestamp, 
            database_content.timestamp
        )
        self.assertEqual(
            unmarshalled_message.is_tombstone, 
            database_content.is_tombstone
        )
        self.assertEqual(
            unmarshalled_message.version_number, 
            database_content.version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, 
            database_content.segment_number
        )
        self.assertEqual(
            unmarshalled_message.segment_count, 
            database_content.segment_count
        )
        self.assertEqual(
            unmarshalled_message.segment_size, 
            database_content.segment_size
        )
        self.assertEqual(
            unmarshalled_message.total_size, 
            database_content.total_size
        )
        self.assertEqual(
            unmarshalled_message.file_adler32, 
            database_content.file_adler32
        )
        self.assertEqual(
            unmarshalled_message.file_md5, 
            database_content.file_md5
        )
        self.assertEqual(
            unmarshalled_message.segment_adler32, 
            database_content.segment_adler32
        )
        self.assertEqual(
            unmarshalled_message.segment_md5, 
            database_content.segment_md5
        )
        self.assertEqual(
            unmarshalled_message.data_content, data_content
        )

    def test_retrieve_key_next(self):
        """test RetrieveKeyNext"""
        request_id = uuid.uuid1().hex
        sequence  = 2
        message = RetrieveKeyNext(
            request_id,
            sequence 
        )
        marshalled_message = message.marshall()
        unmarshalled_message = RetrieveKeyNext.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.sequence, sequence)

    def test_retrieve_key_next_reply_ok(self):
        """test RetrieveKeyNextReply"""
        data_content = random_string(64 * 1024) 
        request_id = uuid.uuid1().hex
        sequence = 42
        result = 0
        message = RetrieveKeyNextReply(
            request_id,
            sequence,
            result,
            data_content
        )
        marshaled_message = message.marshall()
        unmarshalled_message = RetrieveKeyNextReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.sequence, sequence)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(
            unmarshalled_message.data_content, data_content
        )

    def test_retrieve_key_final(self):
        """test RetrieveKeyFinal"""
        request_id = uuid.uuid1().hex
        sequence  = 2
        message = RetrieveKeyFinal(
            request_id,
            sequence 
        )
        marshalled_message = message.marshall()
        unmarshalled_message = RetrieveKeyFinal.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.sequence, sequence)

    def test_retrieve_key_final_reply_ok(self):
        """test RetrieveKeyFinalReply"""
        data_content = random_string(64 * 1024) 
        request_id = uuid.uuid1().hex
        sequence = 452
        result = 0
        message = RetrieveKeyFinalReply(
            request_id,
            sequence,
            result,
            data_content
        )
        marshaled_message = message.marshall()
        unmarshalled_message = RetrieveKeyFinalReply.unmarshall(
            marshaled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.sequence, sequence)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(
            unmarshalled_message.data_content, data_content
        )

    def test_destroy_key(self):
        """test DestroyKey"""
        request_id = uuid.uuid1().hex
        avatar_id  = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        key = "test.key"
        version_number = 0
        segment_number = 6
        message = DestroyKey(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            key,
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DestroyKey.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )

    def test_destroy_key_reply_ok(self):
        """test DestroyKeyReply"""
        request_id = uuid.uuid1().hex
        result = 0
        total_size = 43L
        message = DestroyKeyReply(
            request_id,
            result,
            total_size
        )
        marshalled_message = message.marshall()
        unmarshalled_message = DestroyKeyReply.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(unmarshalled_message.total_size, total_size)

    def test_purge_key(self):
        """test PurgeKey"""
        request_id = uuid.uuid1().hex
        avatar_id  = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        key = "test.key"
        version_number = 0
        segment_number = 6
        message = PurgeKey(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            key,
            version_number,
            segment_number,
        )
        marshalled_message = message.marshall()
        unmarshalled_message = PurgeKey.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )

    def test_purge_key_reply_ok(self):
        """test PurgeKeyReply"""
        request_id = uuid.uuid1().hex
        result = 0
        message = PurgeKeyReply(
            request_id,
            result
        )
        marshalled_message = message.marshall()
        unmarshalled_message = PurgeKeyReply.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_process_status(self):
        """test ProcessStatus"""
        timestamp = time.time()
        exchange = "test-exchange"
        routing_header = "test-routing-header"
        status = "test_status"
        message = ProcessStatus(
            timestamp,
            exchange,
            routing_header,
            status
        )
        marshalled_message = message.marshall()
        unmarshalled_message = ProcessStatus.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.exchange, exchange)
        self.assertEqual(unmarshalled_message.routing_header, routing_header)
        self.assertEqual(unmarshalled_message.status, status)

    def test_hinted_handoff(self):
        """test HintedHandoff"""
        request_id = uuid.uuid1().hex
        avatar_id  = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        timestamp = time.time()
        key = "test.key"
        version_number = 0
        segment_number = 6
        dest_exchange = "dest-exchange"
        message = HintedHandoff(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header,
            timestamp,
            key,
            version_number,
            segment_number,
            dest_exchange
        )
        marshalled_message = message.marshall()
        unmarshalled_message = HintedHandoff.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.segment_number, segment_number
        )
        self.assertEqual(unmarshalled_message.dest_exchange, dest_exchange)

    def test_hinted_handoff_reply_ok(self):
        """test HintedHandoffReply"""
        request_id = uuid.uuid1().hex
        result = 0
        total_size = 43L
        message = HintedHandoffReply(
            request_id,
            result,
            total_size
        )
        marshalled_message = message.marshall()
        unmarshalled_message = HintedHandoffReply.unmarshall(marshalled_message)
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_space_accounting_detail(self):
        """test SpaceAccountingDetail"""
        avatar_id = 1001
        timestamp = time.time()
        event = SpaceAccountingDetail.bytes_added
        value = 42

        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            event,
            value
        )
        marshalled_message = message.marshall()
        unmarshalled_message = SpaceAccountingDetail.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.event, event)
        self.assertEqual(unmarshalled_message.value, value)

    def test_rebuild_request(self):
        """test RebuildRequest"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        message = RebuildRequest(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = RebuildRequest.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_rebuild_reply(self):
        """test RebuildReply"""
        request_id = uuid.uuid1().hex
        result = 0
        message = RebuildReply(request_id, result)
        marshalled_message = message.marshall()
        unmarshalled_message = RebuildReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)

    def test_stat(self):
        """test Stat"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        key = "aaa/bbb"
        version_number = 0
        message = Stat(
            request_id,
            avatar_id,
            key,
            version_number,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = Stat.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(unmarshalled_message.key, key)
        self.assertEqual(
            unmarshalled_message.version_number, version_number
        )
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_stat_reply(self):
        """test StatReply"""
        request_id = uuid.uuid1().hex
        result = 0
        timestamp = time.time()
        total_size = 1024 * 1024 * 1024
        file_adler = -1
        file_md5 = "ABCDEF0123456789"
        userid = 0001
        groupid = 0002
        permissions = 0666

        message = StatReply(
            request_id, 
            result,
            timestamp,
            total_size,
            file_adler,
            file_md5,
            userid,
            groupid,
            permissions
        )
        marshalled_message = message.marshall()
        unmarshalled_message = StatReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(unmarshalled_message.timestamp, timestamp)
        self.assertEqual(unmarshalled_message.total_size, total_size)
        self.assertEqual(unmarshalled_message.file_adler, file_adler)
        self.assertEqual(unmarshalled_message.file_md5, file_md5)
        self.assertEqual(unmarshalled_message.userid, userid)
        self.assertEqual(unmarshalled_message.groupid, groupid)
        self.assertEqual(unmarshalled_message.permissions, permissions)

    def test_space_usage(self):
        """test SpaceUsage"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-header"
        message = SpaceUsage(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header
        )
        marshalled_message = message.marshall()
        unmarshalled_message = SpaceUsage.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.avatar_id, avatar_id)
        self.assertEqual(
            unmarshalled_message.reply_routing_header, 
            reply_routing_header
        )
        self.assertEqual(
            unmarshalled_message.reply_exchange, reply_exchange
        )

    def test_space_usage_reply(self):
        """test SpaceUsageReply"""
        request_id = uuid.uuid1().hex
        result = 0
        bytes_added = 1024 * 1024 * 1024
        bytes_removed = 1024 * 1024
        bytes_retrieved = 512 * 1024 * 1204

        message = SpaceUsageReply(
            request_id, 
            result,
            bytes_added,
            bytes_removed,
            bytes_retrieved
        )
        marshalled_message = message.marshall()
        unmarshalled_message = SpaceUsageReply.unmarshall(
            marshalled_message
        )
        self.assertEqual(unmarshalled_message.request_id, request_id)
        self.assertEqual(unmarshalled_message.result, result)
        self.assertEqual(unmarshalled_message.bytes_added, bytes_added)

if __name__ == "__main__":
    unittest.main()

