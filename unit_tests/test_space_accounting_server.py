# -*- coding: utf-8 -*-
"""
test_space_accounting_server.py

test space accounting
"""
import logging
import os
import os.path
import shutil
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging
from messages.space_accounting_detail import SpaceAccountingDetail

from unit_tests.util import generate_key, generate_database_content

_log_path = "/var/log/pandora/test_space_accounting_server.log"

from diyapi_space_accounting_server.diyapi_space_accounting_server_main import \
    _handle_detail

class TestDatabaseServer(unittest.TestCase):
    """test message handling in database server"""

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        pass

    def test_detail(self):
        """test various forms of SpaceAccountingDetail"""
        avatar_id = 1001
        bytes_added = 42

        message = SpaceAccountingDetail(
            avatar_id, 
            time.time(),
            SpaceAccountingDetail.bytes_added,
            bytes_added
        )
            
        state = dict() 
        _handle_detail(state, message.marshall())

        self.assertEqual(state.has_key(avatar_id), True, state)
        self.assertEqual(state[avatar_id].has_key("bytes_added"), True, state)
        self.assertEqual(state[avatar_id]["bytes_added"], bytes_added, state)

if __name__ == "__main__":
    unittest.main()

