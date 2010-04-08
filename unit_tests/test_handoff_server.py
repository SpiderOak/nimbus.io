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

from diyapi_data_writer.diyapi_data_writer_main import _routing_header \
        as data_writer_routing_header

from diyapi_handoff_server.diyapi_handoff_server_main import \
        _handle_hinted_handoff, \
        _handle_process_status
from diyapi_handoff_server.hint_repository import HintRepository

from unit_tests.archive_util import archive_small_content, \
        archive_large_content

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
        content = random_string(64 * 1024) 
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        segment_number = 2
        timestamp = time.time()
        archive_small_content(
            self, 
            avatar_id, 
            key, 
            version_number, 
            segment_number, 
            content,
            timestamp=timestamp
        )

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
        archive_large_content(
            self, 
            avatar_id, 
            key, 
            version_number,
            segment_number, 
            segment_size, 
            total_size, 
            test_data
        )    


if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()
