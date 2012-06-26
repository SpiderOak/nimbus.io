# -*- coding: utf-8 -*-
"""
greenlet_database_util.py

Utility routines for using psycopg2 with greenlets
"""
import psycopg2.extensions

class GetConnection(object):
    """
    Context Manager for connection pool
    """
    def __init__(self, connection_pool):
        self._connection_pool = connection_pool
        self._active_connection = None

    def __enter__(self):
        self._active_connection = self._connection_pool.getconn()
        self._active_connection.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        return self._active_connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        # we assume the caller has committed if they are going to
        self._active_connection.rollback()
        self._connection_pool.putconn(self._active_connection)


class WithConnection(object):
    """
    decorator for the connection pool
    supplies connection as keyword argument 'connection'
    """
    def __init__(self, connection_pool_getter):
        self._connection_pool_getter = connection_pool_getter

    def __call__(self, function):
        connection_pool_getter = self._connection_pool_getter
        def wrap(*args, **kwargs):
            connection_pool = connection_pool_getter()
            connection = connection_pool.getconn()
            connection.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
            kwargs["connection"] = connection

            try:
                return function(*args, **kwargs)
            finally:
                # we assume the caller has committed if they are going to
                connection.rollback()
                connection_pool.putconn(connection)

        return wrap
            
