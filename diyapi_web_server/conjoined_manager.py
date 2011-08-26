# -*- coding: utf-8 -*-
"""
conjoined_manager.py

functions for conjoined archive data
"""
_list_conjoined_query = """
    select identifier, key from diy_central.conjoined where
    collection_id = %s
""".strip()

def list_conjoined_archives(connection, collection_id):
    """
    get a list of tuples for all  active conjoiined archives
    """
    return connection.fetch_all_rows(
        _list_conjoined_query, [collection_id, ]
    )


