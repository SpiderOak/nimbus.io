# -*- coding: utf-8 -*-
"""
node_databases.py

connections to (remote) node databases
"""
import os
import psycopg2
import psycopg2.extras

from tools.database_connection import get_node_database_dsn

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_node_database_hosts = \
    os.environ["NIMBUSIO_NODE_DATABASE_HOSTS"].split()
_node_database_ports = \
    os.environ["NIMBUSIO_NODE_DATABASE_PORTS"].split()
_node_database_passwords = \
    os.environ["NIMBUSIO_NODE_USER_PASSWORDS"].split() 

def get_node_databases(exclude_names=[]):
    """
    return a dict of database connections keyed by node name
    """
    node_databases = dict()
    node_database_list = zip(_node_names, 
                             _node_database_hosts,
                             _node_database_ports,
                             _node_database_passwords)

    for name, host, port, password in node_database_list:
        if name in exclude_names:
            continue
        host='localhost'
        dsn = get_node_database_dsn(name, password, host, port)
        connection = psycopg2.connect(dsn)
        node_databases[name] = connection

    return node_databases       

