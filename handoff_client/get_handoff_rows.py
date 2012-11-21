# -*- coding: utf-8 -*-
"""
segment_rows.py

retrieve segment rows to be handed off
"""
import base64

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
            # gotta jump through hoops to get base64 on python 3
            encoded_bytes = base64.b64encode(bytes(row["file_hash"]))
            encoded_string = str(encoded_bytes)
            # this gives us a string of the form "b'<data>'"
            # what we want is <data>
            row["file_hash"] = encoded_string[2:-1]
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

    for node_name, connection in node_databases.items():
        cursor = connection.cursor()
        conjoined_rows_for_node = _retrieve_conjoined_handoffs(cursor, node_id)
        segment_rows_for_node = _retrieve_segment_handoffs(cursor, node_id)
        cursor.close()

        conjoined_rows.extend([(node_name, r) for r in conjoined_rows_for_node])
        segment_rows.extend([(node_name, r) for r in segment_rows_for_node])
    
    return conjoined_rows, segment_rows

