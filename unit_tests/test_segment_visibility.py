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
from tools.database_connection import _node_database_name, \
    _node_database_user, \
    get_node_connection 

from segment_visibility.sql_factory import collectable_archive, \
    list_versions, \
    list_keys, \
    version_for_key

_node_name = "test"
_database_password = "test_password"
_database_host = os.environ.get("NIMBUSIO_NODE_DATABASE_HOST", "localhost")
_database_port = int(os.environ.get("NIMBUSIO_NODE_DATABASE_PORT", "5432"))

_test_collection_id = 1
_test_key = 'key-10'
_test_prefix = 'key-1'
_test_unified_id = None    # define later
_test_no_such_unified_id = 1

def _initialize_logging_to_stderr():
    from tools.standard_logging import _log_format_template
    log_level = logging.DEBUG
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _install_schema_and_test_data():
    log = logging.getLogger("_install_schema")
    database_name = _node_database_name(_node_name)
    user_name = _node_database_user(_node_name)

    sql_path = identify_program_dir("sql")
    schema_path = os.path.join(sql_path, "nimbusio_node.sql")

    env = {"PGPASSWORD" : _database_password};
    args = ["/usr/bin/psql", 
            "-h", _database_host,
            "-p", str(_database_port),
            "-d", database_name, 
            "-U", user_name,
            "-f", schema_path]
    log.debug(args)

    process = subprocess.Popen(args, env=env)
    process.wait()
    assert process.returncode == 0, process.returncode

    test_data_path = os.path.join(sql_path, "test_gc.sql")
    env = {"PGPASSWORD" : _database_password};
    args = ["/usr/bin/psql", 
            "-h", _database_host,
            "-p", str(_database_port),
            "-d", database_name, 
            "-U", user_name,
            "-f", test_data_path]
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
        log.debug("installing schema and test data")
        _install_schema_and_test_data()
        log.debug("creating database connection")
        self._connection = get_node_connection(_node_name, 
                                               _database_password, 
                                               _database_host, 
                                               _database_port)
        log.debug("setup done")

    def tearDown(self):
        log = logging.getLogger("tearDown")        
        log.debug("teardown starts")
        if hasattr(self, "_connection"):
            self._connection.close()
            delattr(self, "_connection")
        log.debug("teardown done")
    
    def test_collectable(self):
        """
        test retrieving garbage collectable segments
        """
        log = logging.getLogger("test_collectable")

        sql_text = collectable_archive(_test_collection_id, 
                                       versioned=False, 
                                       key=_test_key, 
                                       unified_id=None)
        log.debug(sql_text)
        args = {"collection_id" : _test_collection_id,
                "versioned"     : False,
                "key"           : _test_key,
                "unified_id"    : None}
        rows = self._connection.fetch_all_rows(sql_text, args)
        log.debug(rows)

        sql_text = collectable_archive(_test_collection_id, 
                                       versioned=True, 
                                       key=_test_key, 
                                       unified_id=_test_no_such_unified_id)
        #log.debug(sql_text)
        self.assertEqual(False, True)

    def xxxtest_list_versions(self):
        """
        test listing the versions of a key
        """
        log = logging.getLogger("test_list_versions")

        # check that there's no more than one row per key for a versioned 
        # collection
        # check that every row begins with prefix
        sql_text = list_versions(_test_collection_id, 
                                 versioned=False, 
                                 prefix=_test_prefix) 
        log.debug(sql_text)

        args = {"collection_id" : _test_collection_id,
                "versioned"     : False,
                "prefix"        : _test_prefix, }

        rows = self._connection.fetch_all_rows(sql_text, args)
        log.debug(rows)

        # check that there's >= as many rows now as above.
        sql_text = list_versions(_test_collection_id, 
                                 versioned=True, 
                                 prefix=_test_prefix)
        log.debug(sql_text)

        args = {"collection_id" : _test_collection_id,
                "versioned"     : False,
                "prefix"        : _test_prefix, }

        rows = self._connection.fetch_all_rows(sql_text, args)
        log.debug(rows)

        self.assertEqual(False, True)

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    unittest.main()

