# -*- coding: utf-8 -*-
"""
database_connecton.py

provide connections to the nimbus.io databases
"""
import os

_central_database_name = "nimbusio_central"
_central_database_user = "nimbusio_central_user"
_node_database_name_prefix = "nimbusio_node"
_node_database_user_prefix = "nimbusio_node_user"

class DatabaseConnection(object):
    """A connection to the nimbus.io databases"""
    def __init__(
        self, 
        database_name, 
        database_user, 
        database_password, 
        database_host,
        database_port
    ):
        """Create an instance of the connection"""
        import psycopg2
        self._connection = psycopg2.connect(
            database=database_name, 
            user=database_user, 
            password=database_password,
            host=database_host, 
            port=database_port
        )
        
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
        """run a statement and return the last row id inserted"""
        cursor = self._connection.cursor()
        cursor.execute(query, *args)
        lastrowid = cursor.lastrowid
        cursor.close()

        return lastrowid
        
    def commit(self):
        """commit any pending transaction"""
        self._connection.commit()

    def rollback(self):
        """roll back any pending transaction"""
        self._connection.rollback()

    def close(self):
        """close the connection"""
        self._connection.close()

def get_central_connection():
    central_database_password = os.environ["NIMBUSIO_CENTRAL_USER_PASSWORD"]
    database_host = os.environ.get(
        "NIMBUSIO_CENTRAL_DATABASE_HOST", "localhost"
    )
    database_port = int(os.environ.get(
        "NIMBUSIO_CENTRAL_DATABASE_PORT", "5432"
    ))
    connection = DatabaseConnection(
        database_name=_central_database_name,
        database_user=_central_database_user,
        database_password=central_database_password,
        database_host=database_host,
        database_port=database_port
    )
    return connection

def get_node_local_connection():
    node_name = os.environ["NIMBUSIO_NODE_NAME"]
    database_name = ".".join([_node_database_name_prefix, node_name, ])
    database_user = ".".join([_node_database_user_prefix, node_name, ])
    database_password = os.environ['NIMBUSIO_NODE_USER_PASSWORD']
    database_host = os.environ.get("NIMBUSIO_NODE_DATABASE_HOST", "localhost")
    database_port = int(os.environ.get("NIMBUSIO_NODE_DATABASE_PORT", "5432"))
    connection = DatabaseConnection(
        database_name=database_name,
        database_user=database_user,
        database_password=database_password,
        database_host=database_host,
        database_port=database_port
    )
    return connection

