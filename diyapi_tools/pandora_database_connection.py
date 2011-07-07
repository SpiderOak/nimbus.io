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

def get_database_connection(user=None, password=None):
    database_name = "pandora"
    database_user = (user if user is not None else "pandora_storage_server")
    database_password = (password if password is not None else \
                         os.environ['PANDORA_DB_PW_pandora_storage_server'])

    database_host = os.environ.get('PANDORA_DATABASE_HOST', 'localhost')
    database_port = int(os.environ.get('PANDORA_DATABASE_PORT', '5432'))
    connection = PandoraDatabaseConnection(
        database_name=database_name,
        database_user=database_user,
        database_password=database_password,
        database_host=database_host,
        database_port=database_port
    )
    return connection

def get_node_local_connection():
    local_database_user = "pandora"
    local_database_password = os.environ['PANDORA_DB_PW_pandora']
    database_host = os.environ.get("SPIDEROAK_LOCAL_DATABASE_HOST", "localhost")
    database_port = int(os.environ.get("SPIDEROAK_LOCAL_DATABASE_PORT", "5432"))
    database_name = "diy.%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
    connection = PandoraDatabaseConnection(
        database_name=database_name,
        database_user=local_database_user,
        database_password=local_database_password,
        database_host=database_host,
        database_port=database_port
    )
    return connection

