# -*- coding: utf-8 -*-
"""
base_lookup.py

See Ticket #45 Cache records from nimbus.io central database in memcached

This is a base class for table lookups, wrapping common functionality
"""
import logging

from tools.data_definitions import memcached_central_key_template

_expiration_time_in_seconds = 24 * 60 * 60 # expiration of 1 day

class BaseLookup(object):
    """
    See Ticket #45 Cache records from nimbus.io central database in memcached

    This is a base class for table lookups, wrapping common functionality
    """
    def __init__(self, 
                 memcached_client, 
                 table_name, 
                 lookup_field_name, 
                 database_lookup_function):

        self._name = table_name
        self._log = logging.getLogger(self._name)

        self._memcached_client = memcached_client
        self._table_name = table_name
        self._lookup_field_name = lookup_field_name
        self._database_lookup_function = database_lookup_function

    def __str__(self):
        return self._name

    def __get_value__(self, lookup_field_value):
        """
        retrieve a dict of column data from memcached, or database
        raise KeyError if not found
        """
        result = self.get(lookup_field_value)
        if result is None:
            raise KeyError("No data for {0}".format(lookup_field_value))
        return result

    def get(self, lookup_field_value):
        """
        retrieve a dict of column data from memcached, or database
        return None if not found
        """
        memcached_key = \
            memcached_central_key_template.format(self._table_name,
                                                  self._lookup_field_name,
                                                  lookup_field_value)

        cached_dict = self._memcached_client.get(memcached_key)
        if cached_dict is not None:
            self._log.debug("cache hit {0}".format(memcached_key))
            return cached_dict

        # cache miss, try the database
        self._log.debug("cache miss {0}".format(memcached_key))
        database_dict = self._database_lookup_function(lookup_field_value)

        if database_dict is not None:
            self._log.debug("database hit {0}".format(memcached_key))
            success = \
                self._memcached_client.set(memcached_key, 
                                           database_dict, 
                                           time=_expiration_time_in_seconds)
            assert success

        if database_dict is None:
            self._log.debug("database miss {0}".format(memcached_key))

        return database_dict

