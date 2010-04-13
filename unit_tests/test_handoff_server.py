# -*- coding: utf-8 -*-
"""
test_handoff_server.py

test the handoff server process
"""
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid

from diyapi_tools import amqp_connection
from diyapi_tools.standard_logging import initialize_logging

from unit_tests.util import random_string, generate_key

from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply
from messages.process_status import ProcessStatus

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply

from messages.purge_key import PurgeKey
from messages.purge_key_reply import PurgeKeyReply

from messages.database_key_purge import DatabaseKeyPurge
from messages.database_key_purge_reply import DatabaseKeyPurgeReply

_log_path = "/var/log/pandora/test_handoff_server.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path
_handoff_database_path = os.path.join(_repository_path, "handoff_database")
_dest_database_path = os.path.join(_repository_path, "dest_database")

from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_purge

from diyapi_data_writer.diyapi_data_writer_main import _routing_header \
        as data_writer_routing_header

from diyapi_data_writer.diyapi_data_writer_main import _handle_purge_key, \
        _handle_key_purge_reply

from diyapi_handoff_server.diyapi_handoff_server_main import \
        _handle_hinted_handoff, \
        _handle_process_status, \
        _handle_retrieve_key_start_reply, \
        _handle_archive_key_final_reply, \
        _handle_purge_key_reply
from diyapi_handoff_server.hint_repository import HintRepository

from unit_tests.archive_util import archive_coroutine
from unit_tests.retrieve_util import retrieve_coroutine

_reply_routing_header = "test_handoff"

class TestHandoffServer(unittest.TestCase):
    """test message handling in handoff server"""

    def setUp(self):
        logging.root.setLevel(logging.DEBUG)
        self.tearDown()
        os.makedirs(_repository_path)
        self._key_generator = generate_key()

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_small_handoff(self):
        """
        test handing off archiving all data for a key in a single message
        """
        content_size = 64 * 1024
        content_item = random_string(content_size) 
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 2
        timestamp = time.time()
        handoff_request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()

        handoff_server_state = dict()
        handoff_server_state["hint-repository"] = HintRepository()

        handoff_data_writer_state = dict()
        handoff_data_reader_state = dict()
        handoff_database_state = {
            _database_cache : dict(),
            "database-path" : _handoff_database_path
        }

        dest_data_writer_state = dict()
        dest_data_reader_state = dict()
        dest_database_state = {
            _database_cache : dict(),
            "database-path" : _dest_database_path
        }

        # the adler32 and md5 hashes should be of the original pre-zefec
        # data segment. We don't have that so we make something up.
        adler32 = -42
        md5 = "ffffffffffffffff"

        message = ArchiveKeyEntire(
            handoff_request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            key, 
            version_number,
            segment_number,
            adler32,
            md5,
            content_item
        )

        # this archiver gets the handoff, as if the dest_archiver is offline
        handoff_archiver = archive_coroutine(
            self, 
            handoff_data_writer_state,
            handoff_database_state,
            message
        )

        reply = handoff_archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        request_id = uuid.uuid1().hex
        senders_exchange = "senders-exchange"
        dest_exchange = "dest-exchange"
        message = HintedHandoff(
            request_id,
            avatar_id,
            senders_exchange,
            _reply_routing_header,
            timestamp,
            key,
            version_number,
            segment_number,
            dest_exchange
        )

        marshalled_message = message.marshall()

        replies = _handle_hinted_handoff(
            handoff_server_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        
        # after a successful handoff, the server should send us
        # HintedHandoffReply
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, senders_exchange)
        self.assertEqual(reply.__class__, HintedHandoffReply)
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0, reply.error_message)

        # now send him a ProcessStatus telling him the data writer at the
        # dest repository is back online
        status_timestamp = time.time()

        message = ProcessStatus(
            status_timestamp, 
            dest_exchange,
            data_writer_routing_header,
            ProcessStatus.status_startup
        )

        marshalled_message = message.marshall()

        replies = _handle_process_status(
            handoff_server_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the handoff server to start retrieving the archive
        # in order to send it to to the data_writer
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply.__class__, RetrieveKeyStart)

        # create a retriever to retrieve from the local data_reader
        handoff_retriever = retrieve_coroutine(
            self,
            handoff_data_reader_state,
            handoff_database_state,
            reply
        )

        reply = handoff_retriever.next()
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.__class__, RetrieveKeyStartReply)

        # pass the reply to the handoff server
        marshalled_message = reply.marshall()
        replies = _handle_retrieve_key_start_reply(
            handoff_server_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the handoff server to send the data it retrieves
        # to the archive server for which it was originally intended 
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, dest_exchange)
        self.assertEqual(reply.__class__, ArchiveKeyEntire)

        # this is the archiver that should have received the data in 
        # the first place
        dest_archiver = archive_coroutine(
            self,
            dest_data_writer_state,
            dest_database_state,
            reply
        )

        reply = dest_archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

        # send the archiver's reply back to the handoff server
        marshalled_message = reply.marshall()
        replies = _handle_archive_key_final_reply(
            handoff_server_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the handoff server to send a purge message
        # to its local data_writer
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply.__class__, PurgeKey)

        # send the message to the data_writer
        marshalled_message = reply.marshall()
        replies = _handle_purge_key(
            handoff_data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the data_writer to send a purge message
        # to its local database server
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply.__class__, DatabaseKeyPurge)

        # send the message to the database server
        marshalled_message = reply.marshall()
        replies = _handle_key_purge(
            handoff_database_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the database server to send a (successful) reply back 
        # to the data writer
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply.__class__, DatabaseKeyPurgeReply)

        # send the reply back to the data_writer
        marshalled_message = reply.marshall()
        replies = _handle_key_purge_reply(
            handoff_data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the data_writer to send a (successful) reply back 
        # to the handoff server
        [(reply_exchange, _reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply.__class__, PurgeKeyReply)

        # send the reply back to the handoff server
        marshalled_message = reply.marshall()
        replies = _handle_purge_key_reply(
            handoff_server_state, marshalled_message
        )

        # we expect no reply, because we are all done
        self.assertEqual(len(replies), 0)

    def test_large_handoff(self):
        """
        test handing off archiving a file that needs more than one message.
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
        handoff_request_id = uuid.uuid1().hex
        test_exchange = "reply-exchange"
        timestamp = time.time()

        # the adler32 and md5 hashes should be of the original pre-zefec
        # data segment. We don't have that so we make something up.
        adler32 = -42
        md5 = "ffffffffffffffff"

        handoff_server_state = dict()
        handoff_server_state["hint-repository"] = HintRepository()

        handoff_data_writer_state = dict()
        handoff_data_reader_state = dict()
        handoff_database_state = {
            _database_cache : dict(),
            "database-path" : _handoff_database_path
        }

        dest_data_writer_state = dict()
        dest_data_reader_state = dict()
        dest_database_state = {
            _database_cache : dict(),
            "database-path" : _dest_database_path
        }

        message = ArchiveKeyStart(
            handoff_request_id,
            avatar_id,
            test_exchange,
            _reply_routing_header,
            timestamp,
            sequence,
            key, 
            version_number,
            segment_number,
            segment_size,
            test_data[0]
        )

        handoff_archiver = archive_coroutine(
            self, 
            handoff_data_reader_state,
            handoff_database_state,
            message
        )   

        reply = handoff_archiver.next()

        self.assertEqual(reply.__class__, ArchiveKeyStartReply)
        self.assertEqual(reply.result, 0)

        for content_item in test_data[1:-1]:
            sequence += 1
            message = ArchiveKeyNext(
                handoff_request_id,
                sequence,
                content_item
            )
            reply = handoff_archiver.send(message)
            self.assertEqual(reply.__class__, ArchiveKeyNextReply)
            self.assertEqual(reply.result, 0)
        
        sequence += 1
        message = ArchiveKeyFinal(
            handoff_request_id,
            sequence,
            total_size,
            adler32,
            md5,
            test_data[-1]
        )

        reply = handoff_archiver.send(message)

        self.assertEqual(reply.__class__, ArchiveKeyFinalReply)
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.previous_size, 0)

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()
