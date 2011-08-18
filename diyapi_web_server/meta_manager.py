# -*- coding: utf-8 -*-
"""
meta_manager.py

functions for accessing meta data
"""
_get_meta_query = """
    select meta_value from diy.meta where
    collection_id = ? and key = ? and meta_key = ?
""".strip()

_list_meta_query = """
    select meta_key, meta_value from diy.meta where
    collection_id = ? and key = ?
""".strip()

def get_meta(connection, collection_id, key, meta_key):
    """get the value for one meta key"""
    meta_row = connection.fetch_one_row(
        _get_meta_query, [collection_id, key, meta_key]
    )
    if meta_row is None:
        return None
    return meta_row[0]

def list_meta(connection, collection_id, key):
    """
    get a list of tuples for all meta data associated wiht the key
    """
    return connection.fetch_all_rows(
        _list_meta_query, [colection_id, key, ]
    )

