# -*- coding: utf-8 -*-
"""
node_databases.py

connections to (remote) node databases
"""
import os
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictConnection

from tools.database_connection import get_node_database_dsn

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_node_database_hosts = \
    os.environ["NIMBUSIO_NODE_DATABASE_HOSTS"].split()
_node_database_ports = \
    os.environ["NIMBUSIO_NODE_DATABASE_PORTS"].split()
_node_database_passwords = \
    os.environ["NIMBUSIO_NODE_USER_PASSWORDS"].split() 

def get_node_databases():
    """
    return a dict of database connections keyed by node name
    """
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

    node_databases = dict()
    node_database_list = zip(_node_names, 
                             _node_database_hosts,
                             _node_database_ports,
                             _node_database_passwords)

    for name, host, port, password in node_database_list:

        # XXX: temporary expedient until boostrap is fixed
        host='localhost'
        dsn = get_node_database_dsn(name, password, host, port)
        connection = RealDictConnection(dsn)
        node_databases[name] = connection

    return node_databases       

