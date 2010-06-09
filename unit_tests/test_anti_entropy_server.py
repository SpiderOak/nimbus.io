# -*- coding: utf-8 -*-
"""
test_anti_entropy_server.py

test the anti entropy server
"""
import logging
import os
import time
import unittest

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.amqp_connection import exchange_template

_log_path = "/var/log/pandora/test_anti_entropy_server.log"

from messages.database_consistency_check import DatabaseConsistencyCheck
from messages.database_consistency_check_reply import \
    DatabaseConsistencyCheckReply

_node_names = ["n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10",]
os.environ["DIY_NODE_EXCHANGES"] = " ".join(
    [exchange_template % (node_name, ) for node_name in _node_names]
)

from diyapi_anti_entropy_server.diyapi_anti_entropy_server_main import \
    _create_state, \
    _start_consistency_check, \
    _handle_database_consistency_check_reply

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

        # send back a reply from each node
        for (_exchange, _routing_key, request, ),  node_name in \
        zip(result, _node_names):
            message = DatabaseConsistencyCheckReply(
                request.request_id,
                node_name,
                DatabaseConsistencyCheckReply.successful,
                valid_hash
            )
            _handle_database_consistency_check_reply(state, message.marshall())

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()

