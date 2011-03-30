# -*- coding: utf-8 -*-
"""
test_anti_entropy_server.py

test the anti entropy server
"""
import logging
import os
import shutil
import subprocess
import sys
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging

from unit_tests.util import identify_program_dir, \
        generate_key, \
        generate_database_content, \
        start_database_server,\
        start_anti_entropy_server, \
        poll_process, \
        terminate_process
from unit_tests.zeromq_util import send_request_and_get_reply

_log_path = "/var/log/pandora/test_anti_entropy_server.log"
_node_names = ["node%02d" % (1+i, ) for i in range(10)]
_local_node_name = _node_names[0]
_test_dir = os.path.join("/tmp", "test_diy_anti_entropy_server")
_repository_dirs = [
    os.path.join(_test_dir, "repository_%s" % (n, )) for n in _node_names
]
_anti_entropy_server_address = "tcp://127.0.0.1:8400"
_database_server_base_port = 8000
_database_server_addresses = [
    "tcp://127.0.0.1:%s" % (_database_server_base_port+i, ) for i in range(10)
]

class TestAntiEntropyServer(unittest.TestCase):
    """test message handling in anti entropy server"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_test_dir)
        for repository_dir in _repository_dirs:
            os.mkdir(repository_dir)
        self._key_generator = generate_key()
        self._database_server_processes = list()
        for node_name, address, repository_path in zip(
            _node_names, _database_server_addresses, _repository_dirs
        ):
            database_server_process = start_database_server(
                node_name, address, repository_path
            )
            time.sleep(1.0)
            poll_result = poll_process(database_server_process)
            self.assertEqual(poll_result, None)
            self._database_server_processes.append(database_server_process)
        self._anti_entropy_server_process = \
            start_anti_entropy_server(
                _local_node_name,
                _anti_entropy_server_address,
                _database_server_addresses
            )

    def tearDown(self):
        if hasattr(self, "_anti_entropy_server_process") \
        and self._anti_entropy_server_process is not None:
            terminate_process(self._anti_entropy_server_process)
            self._anti_entropy_server_process = None
        if hasattr(self, "_database_server_processes") \
        and self._database_server_processes is not None:
            for database_server_process in self._database_server_processes:
                terminate_process(database_server_process)
            self._database_server_processes = None
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def _insert_key(self, database_server_address, avatar_id, key, content):
        request_id = uuid.uuid1().hex
        message = {
            "message-type"      : "key-insert",
            "request-id"        : request_id,
            "avatar-id"         : avatar_id,
            "key"               : key, 
            "database-content"  : dict(content._asdict().items())
        }

        reply = send_request_and_get_reply(database_server_address, message)
        self.assertEqual(reply["request-id"], request_id)

        return reply

    def test_simple_check(self):
        """test a simple consistency check"""
        avatar_id = 1001

        # send a simple item to every server
        key  = self._key_generator.next()
        content = generate_database_content()

        for database_server_address in _database_server_addresses:
            reply = self._insert_key(
                database_server_address, avatar_id, key, content
            )
            self.assertEqual(
                reply["result"], "success", reply["error-message"]
            )

        #request a consistency check
        request_id = uuid.uuid1().hex
        message = {
            "message-type"  : "anti-entropy-audit-request",
            "request-id"    : request_id,
            "avatar-id"     : avatar_id,
        }
        reply = send_request_and_get_reply(
            _anti_entropy_server_address, message
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["result"], "success", reply["error-message"])

    def test_mismatch_reply(self):
        """test getting a hash that doesn't match"""
        avatar_id = 1001

        # send a simple item to every server
        key  = self._key_generator.next()
        content = generate_database_content()
        bad_content = content._replace(file_md5="AAAAAAAAAAAAAAAA")

        for database_server_address in _database_server_addresses[:-1]:
            reply = self._insert_key(
                database_server_address, avatar_id, key, content
            )
            self.assertEqual(
                reply["result"], "success", reply["error-message"]
            )
        reply = self._insert_key(
            _database_server_addresses[-1], 
            avatar_id, 
            key, 
            bad_content
        )
        self.assertEqual(
            reply["result"], "success", reply["error-message"]
        )

        #request a consistency check
        request_id = uuid.uuid1().hex
        message = {
            "message-type"  : "anti-entropy-audit-request",
            "request-id"    : request_id,
            "avatar-id"     : avatar_id,
        }
        reply = send_request_and_get_reply(
            _anti_entropy_server_address, message
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertNotEqual(reply["result"], "success")
        self.assertTrue(
            "hash mismatch" in reply["error-message"], reply["error-message"]
        )

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

