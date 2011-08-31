# -*- coding: utf-8 -*-
"""
collection.py

tools for managing the colection table
"""
import re

_default_collection_prefix = "d"

_collection_name_re = re.compile(r'[a-z0-9][a-z0-9-]*[a-z0-9]$')
_max_collection_name_size = 63

def valid_collection_name(collection_name):
    """
    return True if the username is valid
    """
    return len(collection_name) <= _max_collection_name_size \
        and not '--' in collection_name \
        and _collection_name_re.match(collection_name) is not None

def get_collection_id_dict(connection, customer_id):
    """
    return a list of (collection, collection_id) for all the collections
    the customer owns
    """
    result = connection.fetch_all_rows("""
        select name, id from nimbusio_central.collection
        where customer_id = %s deletion_time is null
    """, [customer_id, ]
    )

    return dict(result)

def create_collection(connection, customer_id, collection_name):
    """
    create a collection for the customer
    """
    assert valid_collection_name(collection_name)
    (row_id, ) = connection.fetch_one_row("""
        insert into nimbusio_central.collection
        (name, customer_id)
        values (%s, %s)
        returning id
    """, [collection_name, customer_id, ]
    )

    return row_id

def create_default_collection(connection, customer_id, username):
    """
    create the customer's default collection, based on username
    """
    collection_name = "-".join([_default_collection_prefix, username, ])
    return create_collection(connection, customer_id, collection_name)

def list_collections(connection, customer_id):
    """
    list all collections for the avatar, for all clusters
    """
    result = connection.fetch_all_rows("""
        select name, creation_time from nimbusio_central.collection   
        where customer_id = %s and deletion_time is null
    """, [customer_id, ]
    )

    return result

def delete_collection(connection, collection_name):
    """
    mark the collection as deleted
    """
    connection.execute("""
        update from nimbusio_central.collection
        set deletion_tme = current_timestamp
        where name = %s
    """, [collection_name, ]
    )

def purge_collection(connection, collection_name):
    """
    really delete the collection: this is intended for use in testing
    """
    connection.execute("""
        delete from nimbusio_central.collection
        where name = %s
    """, [collection_name, ]
    )

