# -*- coding: utf-8 -*-
"""
archive_util.py

This is a form of mixin, extracting common code from test_data_writer and
test_data_reader
"""
import hashlib
import time
import uuid
import zlib

from tools import amqp_connection

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_archive_key_entire, _handle_key_insert_reply
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_insert
from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_key_insert_reply, \
        _handle_archive_key_start, \
        _handle_archive_key_next, \
        _handle_archive_key_final

def archive_small_content(self, avatar_id, key, content):
    """
    utility function to push content all the way through the archive process
    This function handles small content: content that can fit in a single
    message
    """
    request_id = uuid.uuid1().hex
    test_exchange = "reply-exchange"
    test_routing_key = "reply.routing-key"
    timestamp = time.time()
    segment_number = 3
    adler32 = zlib.adler32(content)
    md5 = hashlib.md5(content).digest()
    message = ArchiveKeyEntire(
        request_id,
        avatar_id,
        test_exchange,
        test_routing_key,
        key, 
        timestamp,
        segment_number,
        adler32,
        md5,
        content
    )
    marshalled_message = message.marshall()

    data_writer_state = dict()
    replies = _handle_archive_key_entire(
        data_writer_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)

    # after a successful write, we expect the data writer to send a
    # database_key_insert to the database server
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
    self.assertEqual(reply_routing_key, DatabaseKeyInsert.routing_key)
    self.assertEqual(reply.__class__, DatabaseKeyInsert)
    self.assertEqual(reply.request_id, request_id)

    # hand off the reply to the database server
    marshalled_message = reply.marshall()
    database_state = {_database_cache : dict()}
    replies = _handle_key_insert(database_state, marshalled_message)
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.request_id, request_id)
    self.assertEqual(reply.result, 0)
    self.assertEqual(reply.previous_size, 0)

    # pass the database server reply back to data_writer
    # we should get a reply we can send to the web api 
    marshalled_message = reply.marshall()
    replies = _handle_key_insert_reply(
        data_writer_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
    self.assertEqual(reply.result, 0)
    self.assertEqual(reply.previous_size, 0)

def archive_large_content(
    self, avatar_id, key, segment_size, total_size, content_list
):
    request_id = uuid.uuid1().hex
    test_exchange = "reply-exchange"
    test_routing_key = "reply.routing-key"
    timestamp = time.time()
    segment_number = 3

    # the adler32 and md5 hashes should be of the original pre-zefec
    # data segment. We don't have that so we make something up.
    adler32 = -42
    md5 = "ffffffffffffffff"

    sequence = 0

    message = ArchiveKeyStart(
        request_id,
        avatar_id,
        test_exchange,
        test_routing_key,
        key, 
        timestamp,
        sequence,
        segment_number,
        segment_size,
        content_list[0]
    )
    marshalled_message = message.marshall()

    data_writer_state = dict()
    replies = _handle_archive_key_start(
        data_writer_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)

    # we should get a successful reply 
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.__class__, ArchiveKeyStartReply)
    self.assertEqual(reply.result, 0)

    # do the interior content
    for content_list_content in content_list[1:-1]:
        sequence += 1

        message = ArchiveKeyNext(
            request_id,
            sequence,
            content_list_content
        )
        marshalled_message = message.marshall()

        replies = _handle_archive_key_next(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we should get a successful reply 
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, ArchiveKeyNextReply)
        self.assertEqual(reply.result, 0)

    # send the last one
    sequence += 1

    message = ArchiveKeyFinal(
        request_id,
        sequence,
        total_size,
        adler32,
        md5,
        content_list[-1]
    )
    marshalled_message = message.marshall()

    replies = _handle_archive_key_final(
        data_writer_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)

    # after a successful write, we expect the data writer to send a
    # database_key_insert to the database server
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
    self.assertEqual(reply_routing_key, DatabaseKeyInsert.routing_key)
    self.assertEqual(reply.__class__, DatabaseKeyInsert)
    self.assertEqual(reply.request_id, request_id)

    # hand off the reply to the database server
    marshalled_message = reply.marshall()
    database_state = {_database_cache : dict()}
    replies = _handle_key_insert(database_state, marshalled_message)
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.request_id, request_id)
    self.assertEqual(reply.result, 0)
    self.assertEqual(reply.previous_size, 0)

    # pass the database server reply back to data_writer
    # we should get a reply we can send to the web api 
    marshalled_message = reply.marshall()
    replies = _handle_key_insert_reply(
        data_writer_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
    self.assertEqual(reply.result, 0)
    self.assertEqual(reply.previous_size, 0)

