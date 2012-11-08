# -*- coding: utf-8 -*-
"""
get_node_ids.py

get a dict cross referencing node ids with node names
"""
import psycopg2
import psycopg2.extras

from tools.database_connection import get_central_database_dsn

def get_node_ids(node_name):
    connection = psycopg2.connect(get_central_database_dsn())
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    query = """select id, name from nimbusio_central.node 
               where cluster_id = 
                   (select cluster_id from nimbusio_central.node
                    where name = %s)"""

    cursor.execute(query, [node_name, ])

    # we assume node-name will never be the same as node-id
    node_dict = dict()
    for entry in cursor.fetchall():
        node_dict[entry["id"]] = entry["name"]
        node_dict[entry["name"]] = entry["id"]

    cursor.close()
    connection.close()

    return node_dict

