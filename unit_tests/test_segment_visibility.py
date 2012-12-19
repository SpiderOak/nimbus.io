# -*- coding: utf-8 -*-
"""
test_segment_visibility.py

see Ticket #67 Continue implementation of segment visibility subsystem
"""
from collections import Counter
import logging
import os
import os.path
import subprocess
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
from psycopg2.extras import RealDictConnection

from tools.database_connection import get_node_database_dsn

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
        self._connection = RealDictConnection(
            get_node_database_dsn(_node_name, 
                                  _database_password, 
                                  _database_host, 
                                  _database_port))
        log.debug("setup done")

    def tearDown(self):
        log = logging.getLogger("tearDown")        
        log.debug("teardown starts")
        if hasattr(self, "_connection"):
            self._connection.close()
            delattr(self, "_connection")
        log.debug("teardown done")

    @unittest.skip("need sql help")
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
        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        rows = cursor.fetchall()
        cursor.close()
        log.debug(rows)

        sql_text = collectable_archive(_test_collection_id, 
                                       versioned=True, 
                                       key=_test_key, 
                                       unified_id=_test_no_such_unified_id)
        #log.debug(sql_text)
        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        rows = cursor.fetchall()
        cursor.close()

        self.assertEqual(False, True)

    @unittest.skip("isolate test")
    def test_list(self):
        """
        test listing keys and versions of keys
        """
        log = logging.getLogger("test_list")

        sql_text = list_versions(_test_collection_id, 
                                 versioned=False, 
                                 prefix=_test_prefix) 

        args = {"collection_id" : _test_collection_id,
                "versioned"     : False,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        unversioned_rows = cursor.fetchall()
        cursor.close()

        # check that there's no more than one row per key for a non-versioned 
        # collection
        # check that every row begins with prefix
        unversioned_key_counts = Counter()
        for row in unversioned_rows:
            unversioned_key_counts[row["key"]] += 1
            self.assertTrue(row["key"].startswith(_test_prefix))
        for key, value in unversioned_key_counts.items():
            self.assertEqual(value, 1, (key, value))

        sql_text = list_versions(_test_collection_id, 
                                 versioned=True, 
                                 prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "versioned"     : True,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        versioned_rows = cursor.fetchall()
        cursor.close()
        
        versioned_key_counts = Counter()
        for row in versioned_rows:
            versioned_key_counts[row["key"]] += 1
            self.assertTrue(row["key"].startswith(_test_prefix))

        # check that there's >= as many rows now as above.
        for key, value in versioned_key_counts.items():
            self.assertTrue(value >= versioned_key_counts[key], (key, value))

        sql_text = list_keys(_test_collection_id, 
                             versioned=False, 
                             prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "versioned"     : False,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        key_unversioned_rows = cursor.fetchall()
        cursor.close()

        # check that the list keys result is the same as list_versions in the
        # unversioned case above (although there could be extra columns.)
        key_unversioned_counts = Counter()
        for row in key_unversioned_rows:
            key_unversioned_counts[row["key"]] += 1
            self.assertTrue(row["key"].startswith(_test_prefix))
        for key, value in key_unversioned_counts.items():
            self.assertEqual(value, 1, (key, value))
        for key_row, version_row in zip(key_unversioned_rows, unversioned_rows):
            self.assertEqual(key_row["key"], version_row["key"])
            self.assertEqual(key_row["unified_id"], version_row["unified_id"])

        sql_text = list_versions(_test_collection_id, 
                                 versioned=True, 
                                 prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "versioned"     : True,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        key_versioned_rows = cursor.fetchall()
        cursor.close()
        
        key_versioned_counts = Counter()
        for row in key_versioned_rows:
            key_versioned_counts[row["key"]] += 1
            self.assertTrue(row["key"].startswith(_test_prefix))

    def test_limits_and_markers(self):
        """
        check that the limits and markers work correctly. 
        perhaps take the result with limit=None, and run a series of queries 
        with limit=1 for each of those rows, checking results.
        """
        log = logging.getLogger("test_limits_and_markers")

        for versioned in [True, False]:
            sql_text = list_keys(_test_collection_id, 
                                 versioned=versioned, 
                                 prefix=_test_prefix)

            args = {"collection_id" : _test_collection_id,
                    "versioned"     : versioned,
                    "prefix"        : _test_prefix, }

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            baseline_rows = cursor.fetchall()
            cursor.close()

            key_marker = None
            for row in baseline_rows:
                sql_text = list_keys(_test_collection_id, 
                                     versioned=versioned, 
                                     prefix=_test_prefix,
                                     key_marker=key_marker,
                                     limit=1)

                args = {"collection_id" : _test_collection_id,
                        "versioned"     : versioned,
                        "prefix"        : _test_prefix, 
                        "key_marker"    : key_marker,
                        "limit"         : 1}

                cursor = self._connection.cursor()
                cursor.execute(sql_text, args)
                test_row = cursor.fetchone()
                cursor.close()
                
                self.assertEqual(test_row["key"], row["key"], 
                                 (test_row["key"], row["key"]))
                self.assertEqual(test_row["unified_id"], row["unified_id"], 
                                 (test_row["unified_id"], row["unified_id"]))

                key_marker = test_row["key"]

        for versioned in [True, False]:
            sql_text = list_versions(_test_collection_id, 
                                 versioned=versioned, 
                                 prefix=_test_prefix)

            args = {"collection_id" : _test_collection_id,
                    "versioned"     : versioned,
                    "prefix"        : _test_prefix, }

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            baseline_rows = cursor.fetchall()
            cursor.close()

            key_marker = None
            version_marker = None
            for row in baseline_rows:
                sql_text = list_versions(_test_collection_id, 
                                     versioned=versioned, 
                                     prefix=_test_prefix,
                                     key_marker=key_marker,
                                     version_marker=version_marker,
                                     limit=1)

                args = {"collection_id" : _test_collection_id,
                        "versioned"     : versioned,
                        "prefix"        : _test_prefix, 
                        "key_marker"    : key_marker,
                        "version_marker": version_marker,
                        "limit"         : 1}

                cursor = self._connection.cursor()
                cursor.execute(sql_text, args)
                test_row = cursor.fetchone()
                cursor.close()
                
                log.info("{0}, {1}".format(test_row["key"], row["key"]))
                log.debug(sql_text)

                self.assertEqual(test_row["key"], row["key"], 
                                 (versioned, test_row["key"], row["key"]))
                self.assertEqual(test_row["unified_id"], row["unified_id"], 
                                 (test_row["unified_id"], row["unified_id"]))

                key_marker = test_row["key"]
                version_marker = test_row["unified_id"]

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    unittest.main()

