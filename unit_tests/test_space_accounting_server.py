# -*- coding: utf-8 -*-
"""
test_space_accounting_server.py

test space accounting
"""
import logging
import os
import os.path
import subprocess
import sys
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging

from diyapi_space_accounting_server.space_accounting_database import \
    SpaceAccountingDatabase

from unit_tests.util import identify_program_dir, \
        poll_process, \
        terminate_process
from unit_tests.zeromq_util import send_to_pipeline, \
    send_request_and_get_reply

_log_path = "/var/log/pandora/test_space_accounting_server.log"
_local_node_name = "node01"
_space_accounting_server_address = os.environ.get(
    "DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESS",
    "ipc:///tmp/diyapi-space-accounting-%s/socket" % (_local_node_name, )
)
_space_accounting_pipeline_address = os.environ.get(
    "DIYAPI_SPACE_ACCOUNTING_PIPELINE_ADDRESS",
    "ipc:///tmp/diyapi-space-accounting-pipeline-%s/socket" % (
        _local_node_name, 
    )
)

_avatar_id = 1001

def _start_space_accounting_server(node_name):
    log = logging.getLogger("_start_space_accounting_server%s" % (node_name, ))
    server_dir = identify_program_dir(u"diyapi_space_accounting_server")
    server_path = os.path.join(
        server_dir, "diyapi_space_accounting_server_main.py"
    )
    
    args = [
        sys.executable,
        server_path,
    ]

    environment = {
        "PYTHONPATH"                        : os.environ["PYTHONPATH"],
        "SPIDEROAK_MULTI_NODE_NAME"         : node_name,
    }        

    log.info("starting %s %s" % (args, environment, ))
    return subprocess.Popen(args, stderr=subprocess.PIPE, env=environment)

def _detail_generator(
    total_bytes_added, total_bytes_removed, total_bytes_retrieved
):

    current_time = time.time()

    for i in xrange(1000):
        message = {
            "message-type"  : "space-accounting-detail",
            "avatar-id"     : _avatar_id,
            "timestamp"     : current_time+i,
            "event"         : "bytes_added",
            "value"         : total_bytes_added / 1000,
        }
        yield message, None

    for i in xrange(50):
        message = {
            "message-type"  : "space-accounting-detail",
            "avatar-id"     : _avatar_id,
            "timestamp"     : current_time+i,
            "event"         : "bytes_removed",
            "value"         : total_bytes_removed / 50,
        }
        yield message, None

    for i in xrange(25):
        message = {
            "message-type"  : "space-accounting-detail",
            "avatar-id"     : _avatar_id,
            "timestamp"     : current_time+i,
            "event"         : "bytes_retrieved",
            "value"         : total_bytes_retrieved / 25,
        }
        yield message, None


class TestSpaceAccountingServer(unittest.TestCase):
    """test message handling in space accounting server"""

    def setUp(self):
        initialize_logging(_log_path)
        self.tearDown()

        # clear out any old stats
        space_accounting_database = SpaceAccountingDatabase()
        space_accounting_database.clear_avatar_stats(_avatar_id)
        space_accounting_database.commit()

        self._space_accounting_server_process = \
            _start_space_accounting_server(_local_node_name)
        poll_result = poll_process(self._space_accounting_server_process)
        self.assertEqual(poll_result, None)

    def tearDown(self):
        if hasattr(self, "_space_accounting_server_process") \
        and self._space_accounting_server_process is not None:
            terminate_process(self._space_accounting_server_process)
            self._space_accounting_server_process = None

    def test_usage(self):
        """test SpaceUsage"""
        total_bytes_added = 42 * 1024 * 1024 * 1000
        total_bytes_removed = 21  * 1024 * 1024 * 50
        total_bytes_retrieved = 66 * 1024 * 1024 * 25
        request_id = uuid.uuid1().hex

        poll_result = poll_process(self._space_accounting_server_process)
        self.assertEqual(poll_result, None)

        send_to_pipeline(
            _space_accounting_server_address,
            _detail_generator(
                total_bytes_added, total_bytes_removed, total_bytes_retrieved
            )
        )

        request = {
            "message-type"  : "space-usage-request",
            "request-id"    : request_id,
            "avatar-id"     : _avatar_id,
        }
        reply = send_request_and_get_reply(
            _space_accounting_server_address, request
        )
        self.assertEqual(reply["request-id"], request_id)
        self.assertEqual(reply["message-type"], "space-usage-reply")
        self.assertEqual(reply["result"], "success")
        self.assertEqual(
            reply["bytes-added"], 
            total_bytes_added, 
            (reply["bytes-added"], total_bytes_added, )
        )
        self.assertEqual(
            reply["bytes-removed"], 
            total_bytes_removed, 
            (reply["bytes-removed"], total_bytes_removed, )
        )
        self.assertEqual(
            reply["bytes-retrieved"], 
            total_bytes_retrieved, 
            (reply["bytes-retrieved"], total_bytes_retrieved, )
        )

if __name__ == "__main__":
    unittest.main()

