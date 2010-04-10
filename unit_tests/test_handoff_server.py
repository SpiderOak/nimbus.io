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

_log_path = "/var/log/pandora/test_handoff_server.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
os.environ["PANDORA_REPOSITORY_PATH"] = _repository_path

from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply
from messages.process_status import ProcessStatus

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_entire import ArchiveKeyEntire

from diyapi_data_writer.diyapi_data_writer_main import _routing_header \
        as data_writer_routing_header

from diyapi_handoff_server.diyapi_handoff_server_main import \
        _handle_hinted_handoff, \
        _handle_process_status, \
        _handle_retrieve_key_start_reply
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
        content = random_string(content_size) 
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 2
        timestamp = time.time()

        archiver = archive_coroutine(
            self, 
            avatar_id, 
            key, 
            version_number, 
            segment_number,
            content_size,
            content_size,
            timestamp
        )

        archiver.next()

        try:
            archiver.send((content, True, ))
        except StopIteration:
            pass

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

        handoff_server_state = dict()
        handoff_server_state["hint-repository"] = HintRepository()
        replies = _handle_hinted_handoff(
            handoff_server_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        
        # after a successful handoff, the server should send us
        # HintedHandoffReply
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
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
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
        self.assertEqual(reply.__class__, RetrieveKeyStart)

        # create a retriever to retrieve from the local data_reader
        retriever = retrieve_coroutine(self, reply)

        reply = retriever.next()
        self.assertEqual(reply.result, 0)
        self.assertEqual(reply.__class__, RetrieveKeyStartReply)

        # pass the reply to the handoff server
        marshalled_message = reply.marshall()
        replies = _handle_retrieve_key_start_reply(
            handoff_server_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we expect the handoff server to send the data it retrieves
        # to the archvie server for which it was originally intended 
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, dest_exchange)
        self.assertEqual(reply.__class__, ArchiveKeyEntire)

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

        archiver = archive_coroutine(
            self, 
            avatar_id, 
            key, 
            version_number,
            segment_number, 
            segment_size, 
            total_size, 
        )    

        archiver.next()

        for test_item in test_data[:-1]:
            archiver.send((test_item, False, ))

        try:
            archiver.send((test_data[-1], True, ))
        except StopIteration:
            pass

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()
