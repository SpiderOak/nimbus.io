# -*- coding: utf-8 -*-
"""
test_space_accounting_server.py

test space accounting
"""
import datetime
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
    _create_state, _floor_hour, _handle_detail

class TestSpaceAccountingServer(unittest.TestCase):
    """test message handling in space accounting server"""

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        pass

    def test_detail(self):
        """test various forms of SpaceAccountingDetail"""
        avatar_id = 1001
        bytes_added = 42
        timestamp = time.time()

        message = SpaceAccountingDetail(
            avatar_id, 
            timestamp,
            SpaceAccountingDetail.bytes_added,
            bytes_added
        )
            
        state = _create_state()
        _handle_detail(state, message.marshall())

        hour = _floor_hour(datetime.datetime.fromtimestamp(timestamp))
        self.assertEqual(state["data"].has_key(hour), True, state)
        hour_data = state["data"][hour]
        self.assertEqual(hour_data.has_key(avatar_id), True, state)
        avatar_data = hour_data[avatar_id]
        self.assertEqual(avatar_data.has_key("bytes_added"), True, state)
        self.assertEqual(avatar_data["bytes_added"], bytes_added, state)

if __name__ == "__main__":
    unittest.main()

