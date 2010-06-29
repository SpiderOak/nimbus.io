# -*- coding: utf-8 -*-
"""
test_anti_entropy_server.py

test the anti entropy server
"""
import logging
import os
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.amqp_connection import exchange_template

_log_path = "/var/log/pandora/test_anti_entropy_server.log"

from messages.anti_entropy_audit_request import AntiEntropyAuditRequest
from messages.database_consistency_check import DatabaseConsistencyCheck
from messages.database_consistency_check_reply import \
    DatabaseConsistencyCheckReply

_node_names = ["n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10",]
os.environ["DIY_NODE_EXCHANGES"] = " ".join(
    [exchange_template % (node_name, ) for node_name in _node_names]
)

from diyapi_anti_entropy_server.diyapi_anti_entropy_server_main import \
    _create_state, \
    _is_request_state, \
    _start_consistency_check, \
    _handle_anti_entropy_audit_request, \
    _handle_database_consistency_check_reply, \
    _timeout_request

_reply_routing_header = "test_archive"

class TestAntiEntropyServer(unittest.TestCase):
    """test message handling in anti entropy server"""

    def setUp(self):
        logging.root.setLevel(logging.DEBUG)
        self.tearDown()

    def tearDown(self):
        pass

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

    def xxxtest_audit_request(self):
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
            result = _handle_database_consistency_check_reply(
                state, message.marshall()
            )
            self.assertEqual(len(result), 0, result)

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
        self.assertEqual(reply.status, 0, reply)        

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

