# -*- coding: utf-8 -*-
"""
collection.py

tools for managing the colection table
"""
from collections import namedtuple
import re

class CollectionError(Exception):
    pass
class UnparseableCollection(CollectionError):
    pass

_default_collection_prefix = "dd"

_host_collection_name_re = re.compile(
    r'(?P<collection_name>[^.]+)\.nimbus.io(?::\d+)?$'
)
_collection_name_re = re.compile(r'[a-z0-9][a-z0-9-]*[a-z0-9]$')
_max_collection_name_size = 63
_collection_entry_template = namedtuple(
    "CollectionEntry",
    ["collection_name", "collection_id", "username"]
)

def get_collection_from_hostname(connection, hostname):
    """
    parse a hostname for collection name, 
    fetch collection_id and username from the database
    raise UnparseableCollection on failure
    """
    match_object = _host_collection_name_re.match(hostname)
    if match_object is None:
        raise UnparseableCollection("regex cannot parse %r" % (hostname, ))
    collection_name = match_object.group("collection_name").lower()
    result = connection.fetch_one_row("""
        select nimbusio_central.collection.id, 
               nimbusio_central.customer.username
        from nimbusio_central.collection inner join nimbusio_central.customer
        on (nimbusio_central.collection.customer_id =
                                      nimbusio_central.customer.id)
        where nimbusio_central.collection.name = %s
          and nimbusio_central.collection.deletion_time is null
          and nimbusio_central.customer.deletion_time is null
    """.strip(), [collection_name, ])
    if result is None:
        raise UnparseableCollection("collection name %r not in database %r" % (
            collection_name, hostname, 
        ))
    (collection_id, username, ) = result
    return _collection_entry_template(
        collection_name=collection_name,
        collection_id=collection_id,
        username=username
    )

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

def create_collection(connection, username, collection_name):
    """
    create a collection for the customer
    """
    assert valid_collection_name(collection_name)
    (row_id, ) = connection.fetch_one_row("""
        insert into nimbusio_central.collection
        (name, customer_id)
        values (%s, 
                (select id from nimbusio_central.customer where username = %s))
        returning id
    """, [collection_name, username, ]
    )

    return row_id

def create_default_collection(connection, customer_id, username):
    """
    create the customer's default collection, based on username
    """
    collection_name = "-".join([_default_collection_prefix, username, ])
    return create_collection(connection, customer_id, collection_name)

def list_collections(connection, username):
    """
    list all collections for the customer, for all clusters
    """
    result = connection.fetch_all_rows("""
        select name, creation_time from nimbusio_central.collection   
        where customer_id = (select id from nimbusio_central.customer 
                                       where username = %s) 
        and deletion_time is null
    """, [username, ]
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

