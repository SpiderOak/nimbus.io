# -*- coding: utf-8 -*-
"""
segment_rows.py

retrieve segment rows to be handed off
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

def _retrieve_conjoined_handoffs(cursor, node_id):
    query = """
        select * from nimbusio_node.conjoined
        where handoff_node_id = %s
        order by unified_id
    """
    cursor.execute(query, [node_id, ])

    conjoined_row_list = list()
    for row in cursor.fetchall():
        # bytea columns come out of the database as buffer objects
        if row["combined_hash"] is not None: 
            row["combined_hash"] = str(row["combined_hash"])
        # row is of type psycopg2.extras.RealDictRow
        # we want an honest dict
        conjoined_row_list.append(dict(row.items()))

    return conjoined_row_list

def _retrieve_segment_handoffs(cursor, node_id):
    query = """
        select * from nimbusio_node.segment 
        where handoff_node_id = %s
        order by timestamp desc
    """
    cursor.execute(query, [node_id, ])

    segment_row_list = list()
    for row in cursor.fetchall():
        # bytea columns come out of the database as buffer objects
        if row["file_hash"] is not None: 
            row["file_hash"] = str(row["file_hash"])
        # row is of type psycopg2.extras.RealDictRow
        # we want an honest dict
        segment_row_list.append(dict(row.items()))

    return segment_row_list

def get_handoff_rows(node_id):
    """
    get segment rows to be handed off
    """
    conjoined_rows = list()
    segment_rows = list()

    node_database_list = zip(_node_names, 
                             _node_database_hosts,
                             _node_database_ports,
                             _node_database_passwords)
    for name, host, port, password in node_database_list:
        dsn = get_node_database_dsn(name, password, host, port)
        connection = psycopg2.connect(dsn)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conjoined_rows.extend(_retrieve_conjoined_handoffs(cursor, node_id))
        segment_rows.extend(_retrieve_segment_handoffs(cursor, node_id))
        cursor.close()
        connection.close()
    
    return conjoined_rows, segment_rows

