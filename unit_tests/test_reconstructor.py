# -*- coding: utf-8 -*-
"""
test_reconstructor.py

test the reconstructor
"""
import os
import os.path
import shutil
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging
from diyapi_tools.amqp_connection import exchange_template

from messages.rebuild_request import RebuildRequest

os.environ["DIYAPI_REPOSITORY_PATH"] = ""
_node_names = ["n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10",]
os.environ["DIY_NODE_EXCHANGES"] = " ".join(
    [exchange_template % (node_name, ) for node_name in _node_names]
)
_exchange_names = os.environ["DIY_NODE_EXCHANGES"].split() 

from diyapi_database_server.diyapi_database_server_main import \
    _handle_avatar_database_request, \
    _database_cache
from diyapi_reconstructor.diyapi_reconstructor_main import \
    _handle_rebuild_request, \
    _handle_database_avatar_database_reply

_log_path = "/var/log/pandora/test_reconstructor.log"
_test_dir = os.path.join("/tmp", "test_reconstructor_dir")
_reply_exchange = "test-exchange"
_reply_routing_header = "test-routing-header"

class NodeSim(object):
    """simulate a node for testing"""
    def __init__(self, node_name):
        self._node_name = node_name
        self._node_path = os.path.join(_test_dir, self._node_name)
        os.makedirs(self._node_path)
        self.database_state = {
            _database_cache : dict(),
            "node-name"     : self._node_name,
            "database-path" : os.path.join(self._node_path, "content.db"),
        }

class ReconstructorNodeSim(NodeSim):
    """simulate a reconstructor node for testing"""
    def __init__(self, node_name):
        super(ReconstructorNodeSim, self).__init__(node_name)
        self.reconstructor_state = {
            "repository-path"   : self._node_path,
            "host"              : "127.0.0.1",
        }

class TestReconstructor(unittest.TestCase):
    """test message handling in reconstructor"""

    def setUp(self):
        initialize_logging(_log_path)
        self.tearDown()
        self._nodes = dict()
        for node_name, exchange_name in zip(
            _node_names[:-1], _exchange_names[:-1]
        ):
            self._nodes[exchange_name] = NodeSim(node_name)

        self._reconstructor_node = ReconstructorNodeSim(_node_names[-1])
        self._nodes[_exchange_names[-1]] = self._reconstructor_node

    def tearDown(self):
        if os.path.exists(_test_dir):
            shutil.rmtree(_test_dir)

    def test_invalid_avatar(self):
        """test an avatar that does not exist"""
        avatar_id = 1001
        reply = self._request_rebuild(avatar_id)
        self.assertEqual(reply.error, True)

    def _request_rebuild(self, avatar_id):
        request_id = uuid.uuid1().hex

        message = RebuildRequest(
            request_id,
            avatar_id,
            _reply_exchange,
            _reply_routing_header
        )

        # we expect a DatabaseAvatarDatabaseRequest message for each node
        result = _handle_rebuild_request(
            self._reconstructor_node.reconstructor_state,
            message.marshall()
        )
        self.assertEqual(len(result), len(_exchange_names))

        # pass the messages on to database servers
        database_replies = list()
        for exchange, _routing_key, request_message in result:
            database_reply = _handle_avatar_database_request(
                self._nodes[exchange].database_state,
                request_message.marshall()
            )
            database_replies.extend(database_reply)

        # pass the replies back to the reconstructor
        # we expect nothing until the last one, then we expect RebuildReply
        for _exchange, _routing_key, reply_message in database_replies[:-1]:
            intermediate_result = _handle_database_avatar_database_reply(
                self._reconstructor_node.reconstructor_state,
                reply_message.marshall()
            )
            self.assertEqual(len(intermediate_result), 0)

        _exchange, _routing_key, reply_message = database_replies[-1]
        final_result = _handle_database_avatar_database_reply(
            self._reconstructor_node.reconstructor_state,
            reply_message.marshall()
        )

        self.assertEqual(len(final_result), 1)

        _exchange, _routing_key, rebuild_reply = final_result
        self.assertEqual(rebuild_reply.__class__.__name__, "RebuildReply")

        # the caller must evaluate the reply
        return rebuild_reply

if __name__ == "__main__":
    unittest.main()

