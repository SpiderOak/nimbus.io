# -*- coding: utf-8 -*-
"""
segment_rows.py

retrieve segment rows to be handed off
"""

import psycopg2.extras

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

def get_handoff_rows(node_databases, node_id):
    """
    get segment rows to be handed off
    """
    conjoined_rows = list()
    segment_rows = list()

    for connection in node_databases.values():
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        conjoined_rows.extend(_retrieve_conjoined_handoffs(cursor, node_id))
        segment_rows.extend(_retrieve_segment_handoffs(cursor, node_id))
        cursor.close()
    
    return conjoined_rows, segment_rows

