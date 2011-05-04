# -*- coding: utf-8 -*-
"""
test_web_database_client.py

test database_server
"""
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid

import gevent
from gevent_zeromq import zmq

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.greenlet_zeromq_pollster import GreenletZeroMQPollster
from diyapi_tools.greenlet_resilient_client import GreenletResilientClient
from diyapi_tools.greenlet_pull_server import GreenletPULLServer
from diyapi_tools.deliverator import Deliverator
from diyapi_database_server import database_content
from diyapi_web_server.database_client import DatabaseClient

from diyapi_web_server.exceptions import StatFailedError

from unit_tests.util import generate_key, generate_database_content, \
        start_database_server, poll_process, terminate_process

_log_path = "/var/log/pandora/test_web_database_client.log"
_test_dir = os.path.join("/tmp", "test_dir")
_repository_path = os.path.join(_test_dir, "repository")
_local_node_name = "node01"
_database_server_address = "tcp://127.0.0.1:8000"
_client_pull_server_address = "tcp://127.0.01:8001"

class TestWebDatabaseClient(unittest.TestCase):
    """test message handling between DatabaseClient and database server"""

    def setUp(self):
        log = logging.getLogger("setUp")
        log.info("starting")

        self.tearDown()

        os.makedirs(_repository_path)
        self._key_generator = generate_key()
        self._database_server_process = start_database_server(
            _local_node_name, _database_server_address, _repository_path
        )
        poll_result = poll_process(self._database_server_process)
        self.assertEqual(poll_result, None)

        self._zeromq_context = zmq.context.Context()
        self._pollster = GreenletZeroMQPollster()
        self._deliverator = Deliverator()

        self._pull_server = GreenletPULLServer(
            self._zeromq_context, 
            _client_pull_server_address,
            self._deliverator
        )
        self._pull_server.register(self._pollster)

        self._client = GreenletResilientClient(
            self._zeromq_context, 
            self._pollster,
            _database_server_address,
            _local_node_name, 
            _client_pull_server_address,
            self._deliverator
        )

        self._database_client = DatabaseClient(
            _local_node_name, self._client
        )
        self._pollster.start()

        log.info("completed")

    def tearDown(self):
        log = logging.getLogger("tearDown")
        log.info("starting")

        if hasattr(self, "_database_client") \
        and self._database_client is not None:
            self._database_client.close()
            self._database_client = None

        if hasattr(self, "_pull_server") \
        and self._pull_server is not None:
            self._pull_server.close()
            self._pull_server = None

        if hasattr(self, "_pollster") \
        and self._pollster is not None:
            self._pollster.kill()
            self._pollster.join(timeout=3.0)
            self.assertTrue(self._pollster.ready())
            self._pollster = None

        if hasattr(self, "_zeromq_context") \
        and self._zeromq_context is not None:
            self._zeromq_context.term()
            self._zeromq_context = None

        if hasattr(self, "_database_server_process") \
        and self._database_server_process is not None:
            terminate_process(self._database_server_process)
            self._database_server_process = None

        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)
        log.info("completed")

    def _insert_key(self, avatar_id, key, content):
        request_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-insert",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
        }

        delivery_channel = self._client.queue_message_for_send(
            message, database_content.marshall(content)
        )
        reply, _data = delivery_channel.get()
        self.assertEqual(reply["request-id"], request_id)

        return reply

    def test_listmatch_empty_database(self):
        """test listmach on an empty database"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        prefix = "xxx"

        key_list = self._database_client.listmatch(
            request_id, avatar_id, prefix
        )

        self.assertEqual(key_list, [])

    def test_listmatch_single_match(self):
        """test listmach where we expect a single match"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        prefix = "xxx"
        key = prefix + "aaaa"
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)
        self.assertEqual(reply["result"], "success")

        key_list = self._database_client.listmatch(
            request_id, avatar_id, prefix
        )

        self.assertEqual(key_list, [key, ])

    def test_stat_empty_database(self):
        """test stat on an empty database"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0

        self.assertRaises(
            StatFailedError,
            self._database_client.stat,
            request_id, avatar_id, key, version_number
        )

    def test_stat_valid_key(self):
        """test stat where we expect a match"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        key  = self._key_generator.next()
        version_number = 0
        content = generate_database_content()

        reply = self._insert_key(avatar_id, key, content)
        self.assertEqual(reply["result"], "success")

        stat_result = self._database_client.stat(
            request_id, avatar_id, key, version_number
        )

        self.assertEqual(stat_result["timestamp"], content.timestamp)
        self.assertEqual(stat_result["total_size"], content.total_size)
        self.assertEqual(stat_result["file_adler32"], content.file_adler32)
        self.assertEqual(stat_result["file_md5"], content.file_md5)
        self.assertEqual(stat_result["groupid"], content.groupid)
        self.assertEqual(stat_result["permissions"], content.permissions)

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

