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
    ["collection_name", "collection_id", "username", "versioning", ]
)

def get_username_and_collection_id(connection, collection_name):
    """
    fetch collection_id and username from the database
    """
    result = connection.fetch_one_row("""
        select nimbusio_central.collection.id, 
               nimbusio_central.customer.username,
               nimbusio_central.collection.versioning
        from nimbusio_central.collection 
        inner join nimbusio_central.customer
        on (nimbusio_central.collection.customer_id =
                                      nimbusio_central.customer.id)
        where nimbusio_central.collection.name = %s
          and nimbusio_central.collection.deletion_time is null
          and nimbusio_central.customer.deletion_time is null
    """.strip(), [collection_name.lower(), ])
    if result is None:
        raise UnparseableCollection(
            "collection name %r not in database" % (collection_name, )
        )
    (collection_id, username, versioning, ) = result
    return _collection_entry_template(
        collection_name=collection_name,
        collection_id=collection_id,
        username=username,
        versioning=versioning
    )

def valid_collection_name(collection_name):
    """
    return True if the username is valid
    """
    return len(collection_name) <= _max_collection_name_size \
        and not '--' in collection_name \
        and _collection_name_re.match(collection_name) is not None

def get_collection_id(connection, collection_name):
    """
    return collection_id for the collection_name
    """
    result = connection.fetch_one_row("""
        select id from nimbusio_central.collection
        where name = %s and deletion_time is null
    """, [collection_name, ]
    )

    if result is None:
        return None

    (collection_id, ) = result
    return collection_id

def create_collection(connection, username, collection_name, versioning):
    """
    create a collection for the customer
    """
    assert valid_collection_name(collection_name)

    # if the collecton already exists, use it
    result = connection.fetch_one_row("""
        select id, creation_time, deletion_time 
        from nimbusio_central.collection
        where name = %s""", [collection_name, ]
    )
    if result is not None:
        (row_id, creation_time, deletion_time) = result
        # if the existing collection is deleted, undelete it
        # XXX review: What?!? This will totally screw up historical data for
        # billing and space accounting. We should rename the existing
        # collection or move it to an archive table, leave it deleted, and
        # create a new one.
        if deletion_time is not None:
            (creation_time, ) = connection.fetch_one_row("""
                update nimbusio_central.collection
                set creation_time = current_timestamp, 
                deletion_time = null, versioning = %s
                where name = %s
                returning creation_time
            """, [versioning, collection_name, ])
        return creation_time

    # XXX: for now just select a cluster at random to assign the collection to.
    # the real management API code needs more sophisticated cluster selection.
    (creation_time, ) = connection.fetch_one_row("""
        insert into nimbusio_central.collection
        (name, customer_id, cluster_id, versioning)
        values (%s, 
                (select id from nimbusio_central.customer where username = %s),
                (select id from nimbusio_central.cluster 
                 order by random() limit 1),
                %s)
        returning creation_time
    """, [collection_name, username, versioning, ]
    )

    return creation_time

def compute_default_collection_name(username):
    """
    return the name of the customer's default collection, based on username
    """
    return "-".join([_default_collection_prefix, username, ])

def create_default_collection(connection, username, versioning):
    """
    create the customer's default collection, based on username
    """
    collection_name = compute_default_collection_name(username)
    return create_collection(connection, username, collection_name, versioning)

def list_collections(connection, username):
    """
    list all collections for the customer, for all clusters
    """
    result = connection.fetch_all_rows("""
        select name, versioning, creation_time from nimbusio_central.collection   
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
        update nimbusio_central.collection
        set deletion_time = current_timestamp
        where name = %s
    """, [collection_name, ]
    )

def set_collection_versioning(connection, collection_name, versioning):
    """
    set the versioning attribute of the collection to True or False
    """
    connection.execute("""
        update nimbusio_central.collection
        set versioning = %s
        where name = %s
    """, [versioning, collection_name, ]
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

