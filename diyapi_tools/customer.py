# -*- coding: utf-8 -*-
"""
customer.py

tools for managing the customer table
"""
import base64
import re
import sys

from diyapi_tools.data_definitions import random_string
from diyapi_tools.collection import create_default_collection

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
    connection.execute("""
        delete from nimbusio_central.customer_key
        where customer_id = (select id from nimbusio_central.customer
                             where username = %s);
        delete from nimbusio_central.customer 
        where username = %s;
    """.strip(), [username, username, ])
    
def create_customer(connection, username):
    """
    create a customer record for this username
    """
    assert valid_username(username)
    (customer_id, ) = connection.fetch_one_row("""
        insert into nimbusio_central.customer (username) values (%s)
        returning id
    """, [username, ])
    create_default_collection(connection, customer_id, username)

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



