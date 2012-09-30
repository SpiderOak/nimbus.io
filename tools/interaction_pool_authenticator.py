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
from tools.collection_access_control import check_access_control, \
        access_allowed, \
        access_requires_password_authentication, \
        access_forbidden
from tools.customer_lookup import CustomerIdLookup
from tools.customer_key_lookup import CustomerKeyLookup

from web_public_reader.util import sec_str_eq

class AuthenticationError(Exception):
    pass
class AccessUnauthorized(AuthenticationError):
    pass
class AccessForbidden(AuthenticationError):
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

    def authenticate(self, collection_name, access_type, req):
        """
        establish that this is a valid user and a valid collection
        return collection_entry if valid
        raise AccessUnauthorized(error_message) if invalid
        """
        collection_row = self._collection_lookup.get(collection_name.lower()) 
        if collection_row is None:
            error_message = "unknown collection {0}".format(collection_name)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        if access_type is not None:
            access_result = \
                check_access_control(access_type, 
                                     req, 
                                     collection_row["access_control"])
            if access_result == access_allowed:
                return collection_row
            if access_result == access_forbidden:
                raise AccessForbidden()
            assert access_result == access_requires_password_authentication

        customer_row = self._customer_lookup.get(collection_row["customer_id"]) 
        if customer_row is None:
            error_message = "unknown customer {0}".format(collection_name)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        try:
            auth_type, auth_string = req.authorization
        except Exception, instance:
            error_message = "invalid req.authorization {0} {1}".format(
                instance, 
                req.authorization)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        if auth_type != 'NIMBUS.IO':
            error_message = "unknown auth_type %r" % (auth_type, )
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        try:
            key_id, signature = auth_string.split(':', 1)
        except Exception, instance:
            error_message = "invalid auth_string {0} {1}".format(
                instance, auth_string)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        try:
            key_id = int(key_id)
        except Exception, instance:
            error_message = "invalid key_id {0} {1}".format(instance, key_id)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)
    
        customer_key_row = self._customer_key_lookup.get(key_id) 
        if customer_key_row is None:
            error_message = "unknown customer key {0}".format(key_id)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        # IRC chat 09-29-2012 Alan points out a user could send us any key
        if customer_key_row["customer_id"] != collection_row["customer_id"]:
            error_message = \
                "customer_id in customer key {0} != collection {1}".format(
                    customer_key_row["customer_id"], collection_row["customer_id"])
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        try:
            string_to_sign = _string_to_sign(customer_row["username"], req)
        except Exception, instance:
            error_message = "_string_to_sign failed {0} {1} {2}".format(
                instance, customer_row["username"], req)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        try:
            timestamp = int(req.headers['x-nimbus-io-timestamp'])
        except Exception, instance:
            error_message = "invalid x-nimbus-io-timestamp {0} {1}".format(
                instance, req.headers)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        # The timestamp must agree within 10 minutes of that on the server
        time_delta = abs(time.time() - timestamp)
        if time_delta > 600:
            error_message = "timestamp out of range {0} {1}".format(
                timestamp, time_delta)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        try:
            signature = a2b_hex(signature)
        except Exception, instance:
            error_message = "a2b_hex(signature) failed {0} {1}".format(
                timestamp, instance)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        expected = hmac.new(str(customer_key_row["key"]), 
                            string_to_sign, 
                            hashlib.sha256).digest()

        if not sec_str_eq(signature, expected):
            error_message = "signature comparison failed {0} {1}".format(
                customer_row["username"], string_to_sign)
            self._log.error(error_message)
            raise AccessUnauthorized(error_message)

        return collection_row

