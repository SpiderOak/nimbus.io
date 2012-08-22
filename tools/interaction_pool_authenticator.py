# -*- coding: utf-8 -*-
"""
intercation_pool_authenticator.py

Authenticates requests
"""
from binascii import a2b_hex
from collections import namedtuple
import hashlib
import hmac
import logging
import time
import urllib

from tools.LRUCache import LRUCache
from web_public_reader.util import sec_str_eq

class AuthenticationError(Exception):
    pass

_max_customer_key_cache_size = 10000
_customer_key_cache_expiration_interval = 60.0 * 60.0 # 1 hour

_collection_entry_template = namedtuple(
    "CollectionEntry",
    ["collection_name", "collection_id", "username", "versioning", ]
)
_customer_key_template = namedtuple("CustomerKey", ["key_id", "key"])

def _string_to_sign(username, req):
    return '\n'.join((
        username,
        req.method,
        req.headers['x-nimbus-io-timestamp'],
        urllib.unquote_plus(req.path_qs),
    ))

class InteractionPoolAuthenticator(object):
    def __init__(self, interaction_pool):
        self._log = logging.getLogger("InteractionPoolAuthenticator")
        self._interaction_pool = interaction_pool
        self._customer_key_cache = LRUCache(_max_customer_key_cache_size)

    def authenticate(self, collection_name, req):
        """
        establish that this is a valid user and a valid collection
        return collection_entry if valid
        return None if invalid
        """
        async_result = self._interaction_pool.run("""
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
        result_list = async_result.get()

        if len(result_list) == 0:
            error_message = "collection name {0} not in database".format(
                collection_name, )
            self._log.error(error_message)
            raise AuthenticationError(error_message)

        result = result_list[0]

        collection_entry = _collection_entry_template(
            collection_name=collection_name,
            collection_id=result["id"],
            username=result["username"],
            versioning=result["versioning"]
        )

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
    
        customer_key = None
        # test for cached authentification
        if collection_entry.username in self._customer_key_cache:
            cached_customer_key, auth_expiration_time = \
                    self._customer_key_cache[collection_entry.username]
            if key_id == cached_customer_key.key_id:
                if time.time() > auth_expiration_time:
                    del self._customer_key_cache[collection_entry.username]
                else:
                    customer_key = cached_customer_key

        if customer_key is None:
            # no cached key, or cache has expired
            # we could just select on id, but we want to make sure this key
            # belongs to this user
            async_result = self._interaction_pool.run("""
                select key from nimbusio_central.customer_key
                where customer_id = (select id from nimbusio_central.customer
                                     where username = %s)
                                     and id = %s
            """, [collection_entry.username, key_id, ])
            result_list = async_result.get()
            if len(result_list) == 0:
                self._log.error("unknown user {0}".format(
                    collection_entry.username))
                return None

            result = result_list[0]
            key = result["key"]
            customer_key = _customer_key_template(key_id=key_id, key=key)

        try:
            string_to_sign = _string_to_sign(collection_entry.username, req)
        except Exception, instance:
            self._log.error("_string_to_sign failed {0} {1} {2}".format(
                instance, collection_entry.username, req))
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

        expected = hmac.new(str(customer_key.key), 
                            string_to_sign, 
                            hashlib.sha256).digest()

        if not sec_str_eq(signature, expected):
            self._log.error("signature comparison failed %r %r" % (
                collection_entry.username, string_to_sign
            ))
            return None

        # cache the authentication results with an expiration time
        self._customer_key_cache[collection_entry.username] = (
            customer_key, 
            time.time() + _customer_key_cache_expiration_interval,
        )

        return collection_entry

