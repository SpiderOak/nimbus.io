# -*- coding: utf-8 -*-
"""
test_segment_visibility.py

see Ticket #67 Continue implementation of segment visibility subsystem
"""
import logging
import os
import os.path
import subprocess
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.process_util import identify_program_dir

_dbname = "nimbusio_test_node"
_username = "nimbusio_test_node_user"
_password = "nimbusio_test_node_password"

def _initialize_logging_to_stderr():
    from tools.standard_logging import _log_format_template
    log_level = logging.DEBUG
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _install_schema():
    log = logging.getLogger("_install_schema")
    sql_path = identify_program_dir("sql")
    schema_path = os.path.join(sql_path, "nimbusio_node.sql")

    env = {"PGPASSWORD" : _password};
    args = ["/usr/bin/psql", 
            "-h", "127.0.0.1", 
            "-d", _dbname, 
            "-U",  _username,
            "-f", schema_path]
    log.debug(args)

    process = subprocess.Popen(args, env=env)
    process.wait()
    assert process.returncode == 0, process.returncode

class TestSegmentVisibility(unittest.TestCase):
    """
    test segment visibility subsystem
    """
    def setUp(self):
        log = logging.getLogger("setUp")
        log.debug("setup starts")
        _install_schema()
        log.debug("setup done")

    def tearDown(self):
        log = logging.getLogger("tearDown")        
        log.debug("teardown starts")
        log.debug("teardown done")
    
    def test_connect(self):
        """
        test that we can connect
        """
        self.assertEqual(False, True)

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    unittest.main()

