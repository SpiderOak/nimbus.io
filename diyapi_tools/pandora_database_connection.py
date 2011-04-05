# -*- coding: utf-8 -*-
"""
database_connecton.py

provice a connection to the SpiderOak pandora database
"""
import os

class PandoraDatabaseConnection(object):
    """A connection to the SpiderOak pandora database"""
    def __init__(self, database_name, database_user, database_password, database_host):
        """Create an instance of the connection"""
        import psycopg2
        self._connection = psycopg2.connect(
            database=database_name, user=database_user, host=database_host, password=database_password
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

def get_pandora_database_connection(database_user=_database_user):
    database_host = os.environ.get('PANDORA_DATABASE_HOST', 'localhost')
    if not database_host:
        database_host = None
    connection = PandoraDatabaseConnection(
        database_name=_database_name,
        database_user=database_user,
        database_password=_database_password,
        database_host=database_host
    )
    return connection

