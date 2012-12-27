# -*- coding: utf-8 -*-
"""
test_segment_visibility.py

see Ticket #67 Continue implementation of segment visibility subsystem

To run this, first create test user and database:
 sudo -u postgres createuser -P nimbusio_node_user_test
 sudo -u postgres createdb -O nimbusio_node_user_test nimbusio_node.test

To work with the generated debug output files, I generally edit them directly
in vim.  You can run them on the command line like this, or just use piping
straight from vim.
 sudo -u postgres psql nimbusio_node.test < /tmp/debug.sql

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
    version_for_key, \
    mogrify

_write_debug_sql = int(os.environ.get("WRITE_DEBUG_SQL", "0"))

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

    def _retrieve_collectables(self, versioned):
        """
        check that none of these rows appear in any other result.
        check that the rows from other results are not included here.
        """
        sql_text = collectable_archive(_test_collection_id, 
                                       versioned=versioned)

        args = {"collection_id" : _test_collection_id,
                }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        rows = cursor.fetchall()
        cursor.close()

        # there should always be some garbage. If there's not, something's
        # wrong.
        self.assertGreater(len(rows), 0)

        return set([(r["key"], r["unified_id"], ) for r in rows])

    #@unittest.skip("isolate test")
    def test_no_such_collectable(self):
        """
        test that there are no collectable rows for a bogus unified_id
        """
        log = logging.getLogger("test_no_such_collectable")

        sql_text = collectable_archive(_test_collection_id, 
                                       versioned=True, 
                                       key=_test_key, 
                                       unified_id=_test_no_such_unified_id)

        args = {"collection_id" : _test_collection_id,
                "key"           : _test_key,
                "unified_id"    : _test_no_such_unified_id}

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        rows = cursor.fetchall()
        cursor.close()
        self.assertEqual(len(rows), 0, rows)

    #@unittest.skip("isolate test")
    def test_list(self):
        """
        test listing keys and versions of keys
        """
        log = logging.getLogger("test_list")

        versioned = False
        sql_text = list_versions(_test_collection_id, 
                                 versioned=versioned, 
                                 prefix=_test_prefix) 

        args = {"collection_id" : _test_collection_id,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        unversioned_rows = cursor.fetchall()
        cursor.close()

        collectable_set = self._retrieve_collectables(versioned)
        test_set = set([(r["key"], r["unified_id"], ) for r in unversioned_rows])
        collectable_intersection = test_set & collectable_set
        self.assertEqual(len(collectable_intersection), 0, 
                         collectable_intersection)

        # check that there's no more than one row per key for a non-versioned 
        # collection
        # check that every row begins with prefix
        unversioned_key_counts = Counter()
        for row in unversioned_rows:
            unversioned_key_counts[row["key"]] += 1
            self.assertTrue(row["key"].startswith(_test_prefix))
        for key, value in unversioned_key_counts.items():
            self.assertEqual(value, 1, (key, value))

        versioned = True
        sql_text = list_versions(_test_collection_id, 
                                 versioned=versioned, 
                                 prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        versioned_rows = cursor.fetchall()
        cursor.close()

        collectable_set = self._retrieve_collectables(versioned)
        test_set = set([(r["key"], r["unified_id"], ) for r in versioned_rows])
        collectable_intersection = test_set & collectable_set
        self.assertEqual(len(collectable_intersection), 0, 
                         collectable_intersection)
        
        versioned_key_counts = Counter()
        for row in versioned_rows:
            versioned_key_counts[row["key"]] += 1
            self.assertTrue(row["key"].startswith(_test_prefix))

        # check that there's >= as many rows now as above.
        for key, value in versioned_key_counts.items():
            self.assertTrue(value >= unversioned_key_counts[key], (key, value))

        # check that the list keys result is the same as list_versions in the
        # unversioned case above (although there could be extra columns.)
        for versioned in [False, True, ]:
            sql_text = list_keys(_test_collection_id, 
                                 versioned=versioned, 
                                 prefix=_test_prefix)

            args = {"collection_id" : _test_collection_id,
                    "prefix"        : _test_prefix, }

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            key_rows = cursor.fetchall()
            cursor.close()

            collectable_set = self._retrieve_collectables(versioned)
            test_set = set([(r["key"], r["unified_id"], ) for r in key_rows])
            collectable_intersection = test_set & collectable_set
            self.assertEqual(len(collectable_intersection), 0, 
                             collectable_intersection)

            key_counts = Counter()
            for row in key_rows:
                key_counts[row["key"]] += 1
                self.assertTrue(row["key"].startswith(_test_prefix))
            for key, value in key_counts.items():
                self.assertEqual(value, 1, (key, value))
            for key_row, version_row in zip(key_rows, unversioned_rows):
                self.assertEqual(key_row["key"], version_row["key"])
                self.assertEqual(key_row["unified_id"], version_row["unified_id"])

    #@unittest.skip("isolate test")
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
                                 prefix=_test_prefix,
                                 limit=None)

            args = {"collection_id" : _test_collection_id,
                    "prefix"        : _test_prefix, }

            if _write_debug_sql:
                with open("/tmp/debug_all.sql", "w") as debug_sql_file:
                    debug_sql_file.write(mogrify(sql_text, args))

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            baseline_rows = cursor.fetchall()
            cursor.close()
            baseline_set = set([(r["key"], r["unified_id"], ) 
                                for r in baseline_rows])
            key_marker = None
            version_marker = None
            for row_idx, row in enumerate(baseline_rows):
                sql_text = list_versions(_test_collection_id, 
                                     versioned=versioned, 
                                     prefix=_test_prefix,
                                     key_marker=key_marker,
                                     version_marker=version_marker,
                                     limit=1)

                args = {"collection_id" : _test_collection_id,
                        "prefix"        : _test_prefix, 
                        "limit"         : 1}

                if key_marker is not None:
                    args["key_marker"] = key_marker
                if version_marker is not None:
                    args["version_marker"] = version_marker

                if _write_debug_sql:
                    debug_filename = "/tmp/debug_%s.sql" % (row_idx, )
                    with open(debug_filename, "w") as debug_sql_file:
                        debug_sql_file.write(mogrify(sql_text, args))

                # this result should always be stable. is it?
                last_time = None
                for _ in range(5):
                    cursor = self._connection.cursor()
                    cursor.execute(sql_text, args)
                    test_row = cursor.fetchone()
                    cursor.close()
                    if last_time is not None:
                        assert test_row == last_time
                    last_time = test_row

                # make sure it's in the result somewhere. below we test if it's
                # in the right order.
                self.assertEqual(
                    (test_row["key"], test_row["unified_id"]) in baseline_set,
                    True)
                
                log.info("{0}, {1}".format(test_row["key"], row["key"]))
                log.debug(sql_text)

                self.assertEqual(test_row["key"], row["key"], 
                                 (row_idx, versioned, test_row["key"], row["key"]))
                self.assertEqual(test_row["unified_id"], row["unified_id"], 
                                 (row_idx, versioned, test_row["unified_id"], row["unified_id"]))

                key_marker = test_row["key"]
                version_marker = test_row["unified_id"]

    #@unittest.skip("isolate test")
    def test_version_for_key(self):
        """
        version_for_key 
        """
        log = logging.getLogger("test_version_for_key")

        # check that for every row in list_keys, calling version_for_key with
        # unified_id=None should return the same row, regardless of it being 
        # versioned or not.
        for versioned in [True, False]:
            sql_text = list_keys(_test_collection_id, 
                                 versioned=versioned, 
                                 prefix=_test_prefix)

            args = {"collection_id" : _test_collection_id,
                    "prefix"        : _test_prefix, }

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            baseline_rows = cursor.fetchall()
            cursor.close()

            for row in baseline_rows:
                sql_text = version_for_key(_test_collection_id, 
                                           versioned=versioned, 
                                           key=row["key"])

                args = {"collection_id" : _test_collection_id,
                        "key"           : row["key"]} 

                cursor = self._connection.cursor()
                if _write_debug_sql:
                    with open("/tmp/debug.sql", "w") as debug_sql_file:
                        debug_sql_file.write(mogrify(sql_text, args))
                cursor.execute(sql_text, args)
                test_rows = cursor.fetchall()
                cursor.close()

                # 2012-12-20 dougfort -- list_keys and list_versions only
                # retrieve one conjoined part, but version_for_key retrieves
                # all conjoined parts. So we may have more than one row here.
                self.assertTrue(len(test_rows) > 0) 
                for test_row in test_rows:
                    self.assertEqual(test_row["key"], row["key"], 
                                     (test_row["key"], row["key"]))
                    self.assertEqual(test_row["unified_id"], row["unified_id"], 
                                     (test_row["unified_id"], row["unified_id"]))

        # check that these return empty
        for versioned in [True, False]:
            sql_text = version_for_key(_test_collection_id, 
                                       versioned=versioned, 
                                       key=_test_key,
                                       unified_id=_test_no_such_unified_id)

            args = {"collection_id" : _test_collection_id,
                    "key"           : row["key"],
                    "unified_id"    : _test_no_such_unified_id} 

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            test_rows = cursor.fetchall()
            cursor.close()
            self.assertEqual(len(test_rows), 0, test_rows)

    #@unittest.skip("isolate test")
    def test_version_for_key_find_all_same_rows(self):
        """
        check that this can find all the same rows list_keys returns
        """
        log = logging.getLogger("test_version_for_key_find_all_same_rows")

        sql_text = list_keys(_test_collection_id, 
                             versioned=False, 
                             prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        list_keys_rows = cursor.fetchall()
        cursor.close()

        for list_keys_row in list_keys_rows:
            sql_text = version_for_key(_test_collection_id, 
                                       versioned=False, 
                                       key=list_keys_row["key"])

            args = {"collection_id" : _test_collection_id,
                    "key"           : list_keys_row["key"], }

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            version_for_key_rows = cursor.fetchall()
            cursor.close()

            self.assertTrue(len(version_for_key_rows) > 0)
            for version_for_key_row in version_for_key_rows:
                self.assertEqual(version_for_key_row["key"],
                                 list_keys_row["key"])
                self.assertEqual(version_for_key_row["unified_id"],
                                 list_keys_row["unified_id"])

    #@unittest.skip("isolate test")
    def test_list_versions_same_rows(self):
        """
        check that this can find all the same rows list_versions returns in the
        versioned case above
        """
        log = logging.getLogger("test_list_versions_same_rows")

        sql_text = list_versions(_test_collection_id, 
                                 versioned=True, 
                                 prefix=_test_prefix) 

        args = {"collection_id" : _test_collection_id,
                "prefix"        : _test_prefix, }

        with open("/tmp/debug.sql", "w") as debug_sql_file:
            debug_sql_file.write(mogrify(sql_text, args))

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        list_versions_rows = cursor.fetchall()
        cursor.close()

        for list_versions_row in list_versions_rows:
            sql_text = version_for_key(_test_collection_id, 
                                       versioned=True,
                                       key=list_versions_row["key"], 
                                       unified_id=list_versions_row["unified_id"]) 

            args = {"collection_id" : _test_collection_id,
                    "key"           : list_versions_row["key"], 
                    "unified_id"    : list_versions_row["unified_id"]}

            cursor = self._connection.cursor()
            cursor.execute(sql_text, args)
            version_for_key_rows = cursor.fetchall()
            cursor.close()

            self.assertTrue(len(version_for_key_rows) > 0, 
                            "{0} {1}".format(args, list_versions_row))
            for version_for_key_row in version_for_key_rows:
                self.assertEqual(version_for_key_row["key"],
                                 list_versions_row["key"])
                self.assertEqual(version_for_key_row["unified_id"],
                                 list_versions_row["unified_id"],
                                 list_versions_row)

    #@unittest.skip("isolate test")
    def test_list_keys_vs_list_versions(self):
        """ 
        check that this can ONLY find the same rows list_versions returns 
        above IF they are also in the result that list_keys returns
        (i.e. some of them should be findable, some not.)
        """
        # Background: list_keys returns the newest version of every key. 
        # list_versions returns every version of every key. 
        # If a collection is unversioned, output from list_keys and list_versions 
        # should find the same rows 
        # (although the output from list_keys has an extra column.) 
        # In other words, in a versioned collection, any version of a key 
        # that isn't the newest version should be unreachable.
        # So, I was imagining the test to do this:
        # 1. get the full output from list_versions(test_colelction_id, versioned=True) and save it.
        # 2. get the full output from list_keys(test_collection_id, versioned=True) and save it
        # 3. compare the results to determine which keys are older versions
        # 4. For each row, call version_for_key with specific unified_id and versioned arguments 
        #    and verify finding (or correctly not finding) the result.
        #
        # Sothe rows that are in the output of list_versions but are NOT in 
        # the output of list_keys should be rows that are older versions. 
        # (You may have to discard that extra column from list_keys before 
        # comparing results.) That's probably worth an assert or two to verify 
        # that assumption once you have the lists.
        # Then, if we call version_for_key on those rows that are only in 
        # list_versions with versioned=False and specify their unified_id when 
        # calling version_for_key, they should not be reachable. 
        # With versioned=True they should be reachable.
        # The rows that were in both list_versions output and list_keys output 
        # should be reachable either with versioned=True or versioned=False.

        # 1. get the full output from list_versions(test_colelction_id, versioned=True) and save it.
        sql_text = list_versions(_test_collection_id, 
                                 versioned=True, 
                                 prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        list_versions_rows = cursor.fetchall()
        cursor.close()

        list_versions_set = set([(r["key"], r["unified_id"], ) \
                                 for r in list_versions_rows])

        # 2. get the full output from list_keys(test_collection_id, versioned=True) and save it
        sql_text = list_keys(_test_collection_id, 
                             versioned=True, 
                             prefix=_test_prefix)

        args = {"collection_id" : _test_collection_id,
                "prefix"        : _test_prefix, }

        cursor = self._connection.cursor()
        cursor.execute(sql_text, args)
        list_keys_rows = cursor.fetchall()
        cursor.close()

        list_keys_set = set([(r["key"], r["unified_id"], ) \
                                 for r in list_keys_rows])

        # 3. compare the results to determine which keys are older versions
        older_version_set = list_versions_set - list_keys_set

        # Sothe rows that are in the output of list_versions but are NOT in 
        # the output of list_keys should be rows that are older versions. 
        # (You may have to discard that extra column from list_keys before 
        # comparing results.) That's probably worth an assert or two to verify 
        # that assumption once you have the lists.
        for list_versions_row in list_versions_rows:
            test_tuple = (list_versions_row["key"], 
                          list_versions_row["unified_id"], )
            self.assertIn(test_tuple, list_versions_set)
            if test_tuple in list_keys_set:
                self.assertNotIn(test_tuple, older_version_set)
            else:
                self.assertIn(test_tuple, older_version_set)

        # 4. For each row, call version_for_key with specific unified_id and versioned arguments 
        #    and verify finding (or correctly not finding) the result.

        # Then, if we call version_for_key on those rows that are only in 
        # list_versions with versioned=False and specify their unified_id when 
        # calling version_for_key, they should not be reachable. 
        # With versioned=True they should be reachable.
        for key, unified_id in older_version_set:
            for versioned in [False, True, ]:
                sql_text = version_for_key(_test_collection_id, 
                                           versioned=versioned, 
                                           key=key,
                                           unified_id=unified_id)

                args = {"collection_id" : _test_collection_id,
                        "key"           : key,
                        "unified_id"    : unified_id} 

                cursor = self._connection.cursor()
                cursor.execute(sql_text, args)
                test_rows = cursor.fetchall()
                cursor.close()

                if not versioned:
                    self.assertEqual(len(test_rows), 0)
                else:
                    self.assertTrue(len(test_rows) > 0)

        # The rows that were in both list_versions output and list_keys output 
        # should be reachable either with versioned=True or versioned=False.
        for key, unified_id in list_keys_set:
            for versioned in [False, True, ]:
                sql_text = version_for_key(_test_collection_id, 
                                           versioned=versioned, 
                                           key=key,
                                           unified_id=unified_id)

                args = {"collection_id" : _test_collection_id,
                        "key"           : key,
                        "unified_id"    : unified_id} 

                cursor = self._connection.cursor()
                cursor.execute(sql_text, args)
                test_rows = cursor.fetchall()
                cursor.close()

                self.assertTrue(len(test_rows) > 0, 
                                "versioned={0} {1}".format(versioned, args))

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    unittest.main()

