# -*- coding: utf-8 -*-
"""
test_reconstructor.py

test the reconstructor
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

from unit_tests.util import generate_key

_log_path = "/var/log/pandora/test_reconstructor.log"

class TestReconstructor(unittest.TestCase):
    """test message handling in reconstructor"""

    def setUp(self):
        logging.root.setLevel(logging.DEBUG)
        self.tearDown()

    def tearDown(self):
        pass

    def test_valid_avatar(self):
        """test an avatar that has no errors"""
        avatar_id = 1001

if __name__ == "__main__":
    unittest.main()

