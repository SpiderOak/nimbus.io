# -*- coding: utf-8 -*-
"""
database_connecton.py

provice a connection to the SpiderOak pandora database
"""
import os

class PandoraDatabaseConnection(object):
    """A connection to the SpiderOak pandora database"""
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
        
    def execute(self, query, *args):
        """run a statement and return the last row id inserted"""
        cursor = self._connection.cursor()
        cursor.execute(query, *args)
        cursor.close()
        
    def commit(self):
        """commit any pending transaction"""
        self._connection.commit()

    def rollback(self):
        """roll back any pending transaction"""
        self._connection.rollback()

    def close(self):
        """close the connection"""
        self._connection.close()
        
_database_name = "pandora"
_database_user = "pandora_storage_server"
_database_password = os.environ['PANDORA_DB_PW_pandora_storage_server']
_local_database_user = "pandora"
_local_database_password = os.environ['PANDORA_DB_PW_pandora']

def get_database_connection():
    database_host = os.environ.get('PANDORA_DATABASE_HOST', 'localhost')
    database_port = int(os.environ.get('PANDORA_DATABASE_PORT', '5432'))
    connection = DatabaseConnection(
        database_name=_database_name,
        database_user=_database_user,
        database_password=_database_password,
        database_host=database_host,
        database_port=database_port
    )
    return connection

def get_node_local_connection():
    database_host = os.environ.get("SPIDEROAK_LOCAL_DATABASE_HOST", "localhost")
    database_port = int(os.environ.get("SPIDEROAK_LOCAL_DATABASE_PORT", "5432"))
    database_name = "diy.%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
    connection = PandoraDatabaseConnection(
        database_name=database_name,
        database_user=_local_database_user,
        database_password=_local_database_password,
        database_host=database_host,
        database_port=database_port
    )
    return connection

