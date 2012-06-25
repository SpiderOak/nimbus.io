# -*- coding: utf-8 -*-
"""
test_greenlet_connection_pool.py
"""

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import psycopg2
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool

from gevent import monkey
# you must use the latest gevent and have c-ares installed for this to work
# with /etc/hosts 
# hg clone https://bitbucket.org/denis/gevent
monkey.patch_all()

import gevent_zeromq
gevent_zeromq.monkey_patch()

import gevent
from  gevent.greenlet import Greenlet

_database_credentials = {
    "database" : "postgres",
}

class WriteGreenlet(Greenlet):
    """
    A Greenlet to run one database transaction

    - Get a connection from the pool.
    - Start a transaction.
    - Modify something
    - Sleep for 3 seconds
    - Commit the transaction
    - Select the modified data from the database, 
      assert that it indeed modified.
    - Return connection to pool
    """
    def __init__(self, connection_pool, test_number, delay_interval):
        Greenlet.__init__(self)
        self._connection_pool = connection_pool
        self._test_number = test_number
        self._delay_interval = delay_interval

    def _run(self):
        connection = self._connection_pool.getconn()
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        cursor = connection.cursor()
        cursor.execute(
            "insert into test_greenlet_table1 (column1) values (%s)",
            [self._test_number])
        cursor.close()
        gevent.sleep(self._delay_interval)
        connection.commit()
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute("select column1 from test_greenlet_table1")
        result = cursor.fetchall()
        cursor.close()
        self._connection_pool.putconn(connection)
        return result
        
class RollbackGreenlet(Greenlet):
    """
    A Greenlet to roll back a transaction

    - Sleeps for 2 second
    - Gets a connection from the pool
    - Starts a transaction
    - Does some selection
    - Rolls back transaction
    - Return connection to pool
    """
    def __init__(self, connection_pool, delay_interval):
        Greenlet.__init__(self)
        self._connection_pool = connection_pool
        self._delay_interval = delay_interval

    def _run(self):
        gevent.sleep(self._delay_interval)
        connection = self._connection_pool.getconn()
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        cursor = connection.cursor()
        cursor.execute("select column1 from test_greenlet_table1")
        cursor.close()
        connection.rollback()
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._connection_pool.putconn(connection)

_test_schema = """
drop table if exists test_greenlet_table1;
create table test_greenlet_table1 (
    column1 int
);
"""
_test_cleanup = """
drop table if exists test_greenlet_table1;
"""

class WriteGreenletConnectionPool(unittest.TestCase):
    """
    test GreenletConnectionPool
    """

    def setUp(self):
        connection = psycopg2.connect(**_database_credentials)
        connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute(_test_schema)
        cursor.close()
        connection.close()

    def tearDown(self):
        connection = psycopg2.connect(**_database_credentials)
        cursor = connection.cursor()
        cursor.execute(_test_cleanup)
        cursor.close()
        connection.close()

    def test_with_greenlets(self):
        """
        test accessing the pool 
        """
        min_connections = 1
        max_connections = 5
        test_number = 42

        connection_pool = ThreadedConnectionPool(min_connections,
                                                 max_connections,
                                                 **_database_credentials)

        test_greenlet = WriteGreenlet(connection_pool, test_number, 3.0)
        rollback_greenlet = RollbackGreenlet(connection_pool, 3.0)

        test_greenlet.start()
        rollback_greenlet.start()

        test_greenlet.join()
        self.assertTrue(test_greenlet.successful())

        rollback_greenlet.join()
        self.assertTrue(rollback_greenlet.successful())

        result = test_greenlet.value
        self.assertEqual(result, [(test_number, )])

        connection_pool.closeall()

if __name__ == "__main__":
    unittest.main()

