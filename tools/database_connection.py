# -*- coding: utf-8 -*-
"""
database_connecton.py

provide connections to the nimbus.io databases
"""
import os
import logging
import time

import psycopg2
import psycopg2.extensions
from psycopg2.extras import LoggingConnection

central_database_name = "nimbusio_central"
central_database_user = "nimbusio_central_user"
node_database_name_prefix = "nimbusio_node"
node_database_user_prefix = "nimbusio_node_user"

class DatabaseConnection(object):
    """A connection to the nimbus.io databases"""
    def __init__(
        self, 
        database_name, 
        database_user, 
        database_password, 
        database_host,
        database_port,
        logger=None
    ):
        """Create an instance of the connection"""
        if logger is not None:
            connection_factory = LoggingConnection
        else:
            connection_factory = None

        self._connection = psycopg2.connect(
            database=database_name, 
            user=database_user, 
            password=database_password,
            host=database_host, 
            port=database_port,
            connection_factory=connection_factory
        )

        if logger:
            self.set_logger(logger)

        self._connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._in_transaction = False
        cursor = self._connection.cursor()
        cursor.execute("set time zone 'UTC'")
        cursor.close()

    def set_logger(self, logger):
        "log all database queries with the provided logger"
        self._connection.initialize(logger)

    @property
    def status(self):
        return self._connection.status
        
    def set_isolation_level(self, *args, **kwargs):
        return self._connection.set_isolation_level(*args, **kwargs)

    def get_transaction_status(self, *args, **kwargs):
        return self._connection.get_transaction_status(*args, **kwargs)

    def fetch_one_row(self, query, *args):
        """run a query and return the contents of one row"""
        cursor = self._connection.cursor()
        cursor.execute(query, *args)
        result = cursor.fetchone()
        cursor.close()
        return result
        
    def fetch_all_rows(self, query, *args):
        """run a query and return the contents of all rows"""
        cursor = self._connection.cursor()
        cursor.execute(query, *args)
        result = cursor.fetchall()
        cursor.close()
        return result

    def generate_all_rows(self, query, *args):
        """
        run a query and return a generator 
        which will get all rows, withpout havin them all
        in memory
        """
        cursor = self._connection.cursor()
        cursor.execute(query, *args)

        result = cursor.fetchmany()
        while len(result) > 0:
            for row in result:
                yield row
            result = cursor.fetchmany()

        cursor.close()
        
    def execute(self, query, *args):
        """run a statement"""
        cursor = self._connection.cursor()
        cursor.execute(query, *args)
        rowcount = cursor.rowcount
        cursor.close()
        return rowcount
        
    def execute_and_return_id(self, query, *args):
        """
        run a statement
        presumably an insert that includes 'returning id'
        return the id
        """
        assert "returning" in query.lower()
        cursor = self._connection.cursor()
        cursor.execute(query, *args)
        (returned_id, ) = cursor.fetchone()
        cursor.close()

        return returned_id

    def begin_transaction(self):
        """
        start a transaction
        """
        self._connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        self._in_transaction = True
        cursor = self._connection.cursor()
        cursor.execute("begin")
        cursor.close()
        
    def commit(self):
        """commit any pending transaction"""
        assert self._in_transaction
        self._connection.commit()
        self._connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._in_transaction = False

    def rollback(self):
        """roll back any pending transaction"""
        assert self._in_transaction
        self._connection.rollback()
        self._connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._in_transaction = False

    def close(self):
        """close the connection"""
        assert not self._in_transaction
        self._connection.close()

def retry_central_connection(retry_delay=1.0, isolation_level=None):
    "retry until we connect to central db"
    log = logging.getLogger("retry_central_connection")
    conn = None
    attempts = 1
    while True:
        try:
            conn = get_central_connection()
            if isolation_level is not None:
                conn.set_isolation_level(isolation_level)
            break
        except psycopg2.OperationalError as err:
            if attempts % 10 == 1:
                log.warn("could not connect: attempt %d %s %s" % (
                    attempts,
                    getattr(err, "pgcode", '-'),
                    getattr(err, "pgerror", '-'), ))
            attempts += 1
            time.sleep(retry_delay)
    return conn

def get_central_connection(logger=None):
    central_database_password = os.environ["NIMBUSIO_CENTRAL_USER_PASSWORD"]
    database_host = os.environ.get(
        "NIMBUSIO_CENTRAL_DATABASE_HOST", "localhost"
    )
    database_port = int(os.environ.get(
        "NIMBUSIO_CENTRAL_DATABASE_PORT", "5432"
    ))
    connection = DatabaseConnection(
        database_name=central_database_name,
        database_user=central_database_user,
        database_password=central_database_password,
        database_host=database_host,
        database_port=database_port,
        logger=logger
    )
    return connection

def _node_database_name(node_name):
    return ".".join([node_database_name_prefix, node_name, ])

def _node_database_user(node_name):
    database_user = "_".join([node_database_user_prefix, node_name, ])
    database_user = database_user.replace("-", "_")
    return database_user

def get_node_connection(
    node_name, database_password, database_host, database_port, logger=None
):
    database_name = _node_database_name(node_name)
    database_user = _node_database_user(node_name)
    connection = DatabaseConnection(
        database_name=database_name,
        database_user=database_user,
        database_password=database_password,
        database_host=database_host,
        database_port=database_port,
        logger=logger
    )
    return connection

def get_node_local_connection(logger=None):
    node_name = os.environ["NIMBUSIO_NODE_NAME"]
    database_password = os.environ['NIMBUSIO_NODE_USER_PASSWORD']
    database_host = os.environ.get("NIMBUSIO_NODE_DATABASE_HOST", "localhost")
    database_port = int(os.environ.get("NIMBUSIO_NODE_DATABASE_PORT", "5432"))

    return get_node_connection(node_name, 
                               database_password, 
                               database_host, 
                               database_port,
                               logger=logger)

def get_central_database_dsn():
    """
    return a Data Source Name (DSN) string for connecting to a database
    - ``dbname`` -- database name (only in 'dsn')
    - ``host`` -- host address (defaults to UNIX socket if not provided)
    - ``port`` -- port number (defaults to 5432 if not provided)
    - ``user`` -- user name used to authenticate
    - ``password`` -- password used to authenticate
    - ``sslmode`` -- SSL mode (see PostgreSQL documentation)
    """
    return " ".join([
        "dbname={0}".format(central_database_name),
        "host={0}".format(os.environ["NIMBUSIO_CENTRAL_DATABASE_HOST"]),
        "port={0}".format(os.environ["NIMBUSIO_CENTRAL_DATABASE_PORT"]),
        "user={0}".format(central_database_user),
        "password={0}".format(os.environ["NIMBUSIO_CENTRAL_USER_PASSWORD"]),
    ])

def get_node_database_dsn(node_name, 
                          database_password, 
                          database_host, 
                          database_port):
    """
    return a Data Source Name (DSN) string for connecting to a database
    - ``dbname`` -- database name (only in 'dsn')
    - ``host`` -- host address (defaults to UNIX socket if not provided)
    - ``port`` -- port number (defaults to 5432 if not provided)
    - ``user`` -- user name used to authenticate
    - ``password`` -- password used to authenticate
    - ``sslmode`` -- SSL mode (see PostgreSQL documentation)
    """
    return " ".join([
        "dbname={0}".format(_node_database_name(node_name)),
        "host={0}".format(database_host),
        "port={0}".format(database_port),
        "user={0}".format(_node_database_user(node_name)),
        "password={0}".format(database_password),
    ])

def get_node_local_database_dsn():
    """
    return a Data Source Name (DSN) string for connecting to a database
    - ``dbname`` -- database name (only in 'dsn')
    - ``host`` -- host address (defaults to UNIX socket if not provided)
    - ``port`` -- port number (defaults to 5432 if not provided)
    - ``user`` -- user name used to authenticate
    - ``password`` -- password used to authenticate
    - ``sslmode`` -- SSL mode (see PostgreSQL documentation)
    """
    node_name = os.environ["NIMBUSIO_NODE_NAME"]
    database_password = os.environ['NIMBUSIO_NODE_USER_PASSWORD']
    database_host = os.environ.get("NIMBUSIO_NODE_DATABASE_HOST", "localhost")
    database_port = int(os.environ.get("NIMBUSIO_NODE_DATABASE_PORT", "5432"))
    return get_node_database_dsn(node_name,
                                 database_password,
                                 database_host,
                                 database_port)

