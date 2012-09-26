# -*- coding: utf-8 -*-
"""
customer_lookup.py

See Ticket #45 Cache records from nimbus.io central database in memcache

Provide read-only access to the nimbusio_central.customer table
with rows cached in memcache
"""
import logging

import psycopg2.extras

from tools.base_lookup import BaseLookup
from tools.data_definitions import http_timestamp_str

_query_timeout = 60.0
_central_pool_name = "default"
_username_query = """select * from nimbusio_central.customer 
                     where username = %s
                     and deletion_time is null"""
_id_query =       """select * from nimbusio_central.customer 
                     where id = %s
                     and deletion_time is null"""
_timestamp_columns = set(["creation_time", "deletion_time", ])

def _process_result_list(result_list):
    if len(result_list) == 0:
        return None

    assert len(result_list) == 1

    result = result_list[0]
    
    return_result = dict()
    for key, value in result.items():
        if key in _timestamp_columns:
            return_result[key] = http_timestamp_str(value)
        else:
            return_result[key] = value

    return return_result

def _lookup_function_closure(interaction_pool, query):
    def __lookup_function(lookup_field_value):
        log = logging.getLogger("CustomerLookup")
        async_result = \
            interaction_pool.run(interaction=query,
                                 interaction_args=[lookup_field_value, ],
                                 pool=_central_pool_name) 
        try:
            result_list = async_result.get(block=True, 
                                           timeout=_query_timeout)
        except Exception:
            log.exception(lookup_field_value)
            raise

        return _process_result_list(result_list)

    return __lookup_function

def _connection_lookup_function_closure(connection, query):
    def __lookup_function(lookup_field_value):
        log = logging.getLogger("CustomerLookup")
        
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query, [lookup_field_value, ])
        result_list = cursor.fetchall()
        cursor.close()

        return _process_result_list(result_list)

    return __lookup_function

class CustomerUsernameConnectionLookup(BaseLookup):
    """
    See Ticket #45 Cache records from nimbus.io central database in memcache

    Provide read-only access to the nimbusio_central.customer table
    with rows cached in memcache
    """
    def __init__(self, memcached_client, connection):
        lookup_function = _connection_lookup_function_closure(connection,
                                                              _username_query)
        super(CustomerUsernameConnectionLookup, self).__init__(
            memcached_client,
            "customer",
            "username",
            lookup_function)

class CustomerIdLookup(BaseLookup):
    """
    See Ticket #45 Cache records from nimbus.io central database in memcache

    Provide read-only access to the nimbusio_central.customer table
    with rows cached in memcache
    """
    def __init__(self, memcached_client, interaction_pool):
        lookup_function = _lookup_function_closure(interaction_pool,
                                                   _id_query)
        super(CustomerIdLookup, self).__init__(memcached_client,
                                               "customer",
                                               "id",
                                               lookup_function)

