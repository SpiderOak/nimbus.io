# -*- coding: utf-8 -*-
"""
customer.py

tools for managing the customer table
"""
import base64
from collections import namedtuple
import re

from tools.data_definitions import random_string
from tools.collection import compute_default_collection_name, \
    valid_collection_name

_max_username_size = 60

_username_re = re.compile(r'[a-z0-9][a-z0-9-]*[a-z0-9]$')

def _generate_key():
    """generate a key string"""
    return base64.b64encode(random_string(32)).rstrip('=')

def valid_username(username):
    """
    return True if the username is valid
    """
    return len(username) <= _max_username_size \
        and not '--' in username \
        and _username_re.match(username) is not None

def purge_customer(connection, username):
    """
    remove a customer and all keys. 
    This is intended mostly for convenience in testing
    """
    result = connection.fetch_one_row("""
        select id as customer_id from nimbusio_central.customer 
        where username = %s;
    """.strip(), [username, ])
    if result is None:
        return
    (customer_id, ) = result
    connection.execute("""
        delete from nimbusio_central.collection 
        where customer_id = %(customer_id)s;
        delete from nimbusio_central.customer_key
        where customer_id = %(customer_id)s;
        delete from nimbusio_central.customer where id = %(customer_id)s;
    """.strip(), {"customer_id" : customer_id})
    
def create_customer(connection, username, versioning):
    """
    create a customer record for this username
    """
    assert valid_username(username)
    connection.execute("""
        insert into nimbusio_central.customer (username) values (%s)
    """, [username, ])
    _create_default_collection(connection, username, versioning)

def add_key_to_customer(connection, username):
    """
    add a key to an existing customer
    return (key_id, key)
    """
    key = _generate_key()
    (key_id, ) = connection.fetch_one_row("""
        insert into nimbusio_central.customer_key (customer_id, key)
        values (
            (select id from nimbusio_central.customer where username = %s),
            %s
        ) returning id;""", [username, key, ])

    return (key_id, key, )

def list_customer_keys(connection, username):
    """
    list pairs of (key_id, key) for customer
    """
    return connection.fetch_all_rows("""
        select id, key from nimbusio_central.customer_key
        where customer_id = (select id from nimbusio_central.customer
                             where username = %s)
    """, [username, ])

def _create_default_collection(connection, username, versioning):
    """
    create the customer's default collection, based on username
    """
    collection_name = compute_default_collection_name(username)
    return _create_collection(connection, 
                              username, 
                              collection_name, 
                              versioning)


def _create_collection(connection, username, collection_name, versioning):
    """
    create a collection for the customer
    """
    assert valid_collection_name(collection_name)

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


