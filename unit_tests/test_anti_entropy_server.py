# -*- coding: utf-8 -*-
"""
test_anti_entropy_server.py

test the anti entropy server
"""
import logging
import os
import subprocess
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging

from unit_tests.util import identify_program_dir, \
        start_database_server,\
        start_data_writer, \
        poll_process, \
        terminate_process
from unit_tests.zeromq_util import send_request_and_get_reply

_log_path = "/var/log/pandora/test_anti_entropy_server.log"
_node_names = " ".join(["node%2d" % (1+i, ) for i in range(10)])
_local_node_name = _node_names[0]
_test_dir = os.path.join("/tmp", "test_diy_anti_entropy_server")
_repository_dirs = [
    os.path.join(_test_dir, "repository_%s" % (n, )) for n in _node_names
]
_anti_entropy_server_address = os.environ.get(
    "DIYAPI_anti_entropy_server_ADDRESS",
    "ipc:///tmp/diyapi-anti-entropy-server-%s/socket" % (_local_node_name, )
)
_database_server_addresses = " ".join(
    ["tcp://127.0.0.1:%s" % (8000+i, ) for i in range(10)]
)

def _start_anti_entropy_server(node_name):
    log = logging.getLogger("_start_anti_entropy_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_anti_entropy_server")
    server_path = os.path.join(
        server_dir, "diyapi_anti_entropy_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                                : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"                 : _local_node_name,
        "DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESSES"  : \
            _database_server_addresses,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

class TestAntiEntropyServer(unittest.TestCase):
    """test message handling in anti entropy server"""

    def setUp(self):
        self.tearDown()
        os.makedirs(_test_dir)
        for repository_dir in _repository_dirs:
            os.mkdir(repository_dir)
        self._database_server_processes = list()
        for node_name, repository_path, addess in zip(
            _node_names, _repository_dirs, _database_server_addresses
        ):
            database_server_process = start_database_server(
                node_name, repository_path, address
            )
            time.sleep(1.0)
            poll_result = poll_process(database_server_process)
            self.assertEqual(poll_result, None)
        self._database_server_processes.append(database_server_process)
        self._anti_entropy_server_process = \
            _start_anti_entropy_server(_local_node_name)

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

    def test_simple_check(self):
        """test a simple consistency check"""
        avatar_id = 1001
        state = _create_state()

        # we expect to send a DatabaseConsistencyCheck to the database_server
        # on every node
        result = _start_consistency_check(state, avatar_id)
        self.assertEqual(len(result), len(_node_names), result)

        valid_hash = "aaaaaaaaaaaaaaaa"

        # send back a successful reply from each node
        for (_exchange, _routing_key, request, ),  node_name in \
        zip(result, _node_names):
            message = DatabaseConsistencyCheckReply(
                request.request_id,
                node_name,
                DatabaseConsistencyCheckReply.successful,
                valid_hash
            )
            _handle_database_consistency_check_reply(state, message.marshall())

    def test_error_reply(self):
        """test getting an error reply"""
        avatar_id = 1001
        state = _create_state()

        # we expect to send a DatabaseConsistencyCheck to the database_server
        # on every node
        result = _start_consistency_check(state, avatar_id)
        self.assertEqual(len(result), len(_node_names), result)

        valid_hash = "aaaaaaaaaaaaaaaa"
        self.assertEqual(len(state["retry-list"]), 0)

        # send back a successful reply from each node
        for (_exchange, _routing_key, request, ),  node_name in \
        zip(result[:-1], _node_names[:-1]):
            message = DatabaseConsistencyCheckReply(
                request.request_id,
                node_name,
                DatabaseConsistencyCheckReply.successful,
                valid_hash
            )
            _handle_database_consistency_check_reply(state, message.marshall())

        (_exchange, _routing_key, request, ) = result[-1]
        message = DatabaseConsistencyCheckReply(
            request.request_id,
            _node_names[-1],
            DatabaseConsistencyCheckReply.error_database_failure,
            error_message="test"
        )

        _handle_database_consistency_check_reply(state, message.marshall())
        self.assertEqual(len(state["retry-list"]), 1)

    def test_mismatch_reply(self):
        """test getting a hash that doesn't match"""
        avatar_id = 1001
        state = _create_state()

        # we expect to send a DatabaseConsistencyCheck to the database_server
        # on every node
        result = _start_consistency_check(state, avatar_id)
        self.assertEqual(len(result), len(_node_names), result)

        valid_hash = "aaaaaaaaaaaaaaaa"
        invalid_hash = "aaaaaaaaaaaaaaab"
        self.assertEqual(len(state["retry-list"]), 0)

        # send back a successful reply from each node
        for (_exchange, _routing_key, request, ),  node_name in \
        zip(result[:-1], _node_names[:-1]):
            message = DatabaseConsistencyCheckReply(
                request.request_id,
                node_name,
                DatabaseConsistencyCheckReply.successful,
                valid_hash
            )
            _handle_database_consistency_check_reply(state, message.marshall())

        (_exchange, _routing_key, request, ) = result[-1]
        message = DatabaseConsistencyCheckReply(
            request.request_id,
            _node_names[-1],
            DatabaseConsistencyCheckReply.successful,
            invalid_hash
        )

        _handle_database_consistency_check_reply(state, message.marshall())
        self.assertEqual(len(state["retry-list"]), 1)

    def test_timeout(self):
        """test not getting all the responses in time"""
        avatar_id = 1001
        state = _create_state()

        # we expect to send a DatabaseConsistencyCheck to the database_server
        # on every node
        result = _start_consistency_check(state, avatar_id)
        self.assertEqual(len(result), len(_node_names), result)

        valid_hash = "aaaaaaaaaaaaaaaa"
        invalid_hash = "aaaaaaaaaaaaaaab"
        self.assertEqual(len(state["retry-list"]), 0)

        # send back a successful reply from each node
        for (_exchange, _routing_key, request, ),  node_name in \
        zip(result[:-1], _node_names[:-1]):
            message = DatabaseConsistencyCheckReply(
                request.request_id,
                node_name,
                DatabaseConsistencyCheckReply.successful,
                valid_hash
            )
            _handle_database_consistency_check_reply(state, message.marshall())

        # fish out the request id
        request_list = [r for r, _ in filter(_is_request_state, state.items())]
        self.assertEqual(len(request_list), 1)
        request_id = request_list[0]

        _timeout_request(request_id, state)
        self.assertEqual(len(state["retry-list"]), 1)

    def test_audit_request(self):
        """test using the AntiEntropyAuditRequest message"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        reply_exchange = "reply-exchange"
        reply_routing_header = "reply-routing-header"
        state = _create_state()

        message = AntiEntropyAuditRequest(
            request_id,
            avatar_id,
            reply_exchange,
            reply_routing_header
        )

        # we expect to send a DatabaseConsistencyCheck to the database_server
        # on every node
        result = _handle_anti_entropy_audit_request(state, message.marshall())
        self.assertEqual(len(result), len(_node_names), result)

        valid_hash = "aaaaaaaaaaaaaaaa"

        # send back a successful reply from each node except the last one
        for (_exchange, _routing_key, request, ),  node_name in \
        zip(result[:-1], _node_names[:-1]):
            message = DatabaseConsistencyCheckReply(
                request.request_id,
                node_name,
                DatabaseConsistencyCheckReply.successful,
                valid_hash
            )
            check_result = _handle_database_consistency_check_reply(
                state, message.marshall()
            )
            self.assertEqual(len(check_result), 0, check_result)

        # send back a successful reply from the last node
        # we expect to get a AntiEntropyAuditReply
        _exchange, _routing_key, request = result[-1]
        node_name = _node_names[-1]
        message = DatabaseConsistencyCheckReply(
            request.request_id,
            node_name,
            DatabaseConsistencyCheckReply.successful,
            valid_hash
        )
        result = _handle_database_consistency_check_reply(
            state, message.marshall()
        )
        self.assertEqual(len(result), 1, result)
        _exchange, _key, reply = result[0]
        self.assertEqual(
            reply.__class__.__name__, "AntiEntropyAuditReply", reply
        )
        self.assertEqual(reply.result, 0, reply)        

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

