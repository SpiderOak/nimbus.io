# -*- coding: utf-8 -*-
"""
process_conjoined_rows.py

process handoffs of conjoined rows
"""

def _key_function(conjoined_row):
    return conjoined_row["unified_id"]

def process_conjoined_rows(halt_event, node_databases, conjoined_rows):
    """
    process handoffs of conjoined rows
    """
    # sort on unified id to bring pairs together
    conjoined_rows.sort(key=_key_function)

