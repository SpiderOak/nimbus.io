# -*- coding: utf-8 -*-
"""
intercation_pool_authenticator.py

Authenticates requests
"""
from binascii import a2b_hex
import hashlib
import hmac
import logging
import time
import urllib

import gevent

from tools.collection_lookup import CollectionLookup
from tools.customer_lookup import CustomerIdLookup
from tools.customer_key_lookup import CustomerKeyLookup

from web_public_reader.util import sec_str_eq

class AuthenticationError(Exception):
    pass

def _string_to_sign(username, req):
    return '\n'.join((
        username,
        req.method,
        req.headers['x-nimbus-io-timestamp'],
        urllib.unquote_plus(req.path_qs),
    ))

class InteractionPoolAuthenticator(object):
    def __init__(self, memcached_client, interaction_pool):
        self._log = logging.getLogger("InteractionPoolAuthenticator")
        self._interaction_pool = interaction_pool
        self._collection_lookup = CollectionLookup(memcached_client,
                                                   interaction_pool)
        self._customer_lookup = CustomerIdLookup(memcached_client,
                                                 interaction_pool)
        self._customer_key_lookup = CustomerKeyLookup(memcached_client,
                                                      interaction_pool)

    def authenticate(self, collection_name, req):
        """
        establish that this is a valid user and a valid collection
        return collection_entry if valid
        return None if invalid
        """
        collection_row = self._collection_lookup.get(collection_name.lower()) 
        if collection_row is None:
            self._log.error("unknown collection {0}".format(collection_name))
            return None

        customer_row = self._customer_lookup.get(collection_row["customer_id"]) 
        if customer_row is None:
            self._log.error("unknown customer {0}".format(collection_name))
            return None

        try:
            auth_type, auth_string = req.authorization
        except Exception, instance:
            self._log.error("invalid req.authorization {0} {1}".format(
                instance, 
                req.authorization))
            return None

        if auth_type != 'NIMBUS.IO':
            self._log.error("unknown auth_type %r" % (auth_type, ))
            return None

        try:
            key_id, signature = auth_string.split(':', 1)
        except Exception, instance:
            self._log.error("invalid auth_string {0} {1}".format(
                instance, auth_string))
            return None

        try:
            key_id = int(key_id)
        except Exception, instance:
            self._log.error("invalid key_id {0} {1}".format(
                instance, key_id))
            return None
    
        customer_key_row = self._customer_key_lookup.get(key_id) 
        if customer_key_row is None:
            self._log.error("unknown customer key {0}".format(key_id))
            return None

        try:
            string_to_sign = _string_to_sign(customer_row["username"], req)
        except Exception, instance:
            self._log.error("_string_to_sign failed {0} {1} {2}".format(
                instance, customer_row["username"], req))
            return None

        try:
            timestamp = int(req.headers['x-nimbus-io-timestamp'])
        except Exception, instance:
            self._log.error("invalid x-nimbus-io-timestamp {0} {1}".format(
                instance, req.headers))
            return None

        # The timestamp must agree within 10 minutes of that on the server
        time_delta = abs(time.time() - timestamp)
        if time_delta > 600:
            self._log.error("timestamp out of range {0} {1}".format(
                timestamp, time_delta))
            return None

        try:
            signature = a2b_hex(signature)
        except Exception, instance:
            self._log.error("a2b_hex(signature) failed {0} {1}".format(
                timestamp, instance))
            return None

        expected = hmac.new(str(customer_key_row["key"]), 
                            string_to_sign, 
                            hashlib.sha256).digest()

        if not sec_str_eq(signature, expected):
            self._log.error("signature comparison failed %r %r" % (
                customer_row["username"], string_to_sign
            ))
            return None

        return collection_row

