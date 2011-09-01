# -*- coding: utf-8 -*-
"""
sql_authenticator.py

Authenticates requests
"""
from binascii import a2b_hex
import hashlib
import hmac
import logging
import time

from diyapi_tools.LRUCache import LRUCache
from diyapi_tools.customer import get_customer_key
from diyapi_web_server.util import sec_str_eq

_max_customer_key_cache_size = 10000
_customer_key_cache_expiration_interval = 60.0 * 60.0 # 1 hour

def _string_to_sign(username, req):
    return '\n'.join((
        username,
        req.method,
        req.headers['x-nimbus-io-timestamp'],
        req.path_qs,
    ))

class SqlAuthenticator(object):
    def __init__(self):
        self._log = logging.getLogger("SqlAuthenticator")
        self._customer_key_cache = LRUCache(_max_customer_key_cache_size)

    def authenticate(self, connection, username, req):
        try:
            auth_type, auth_string = req.authorization
        except TypeError:
            return False

        if auth_type != 'NIMBUS.IO':
            self._log.debug("unknown auth_type %r" % (auth_type, ))
            return False

        try:
            key_id, signature = auth_string.split(':', 1)
        except TypeError:
            return False

        try:
            key_id = int(key_id)
        except (TypeError, ValueError):
            return False
    
        customer_key = None
        # test for cached authentification
        if username in self._customer_key_cache:
            cached_customer_key, auth_expiration_time = \
                    self._customer_key_cache[username]
            if key_id == cached_customer_key.key_id:
                if time.time() > auth_expiration_time:
                    del self._customer_key_cache[username]
                else:
                    customer_key = cached_customer_key

        if customer_key is None:
            # no cached key, or cache has expired
            customer_key = get_customer_key(connection, username, key_id)
            if customer_key is None:
                self._log.debug("unknown user %r" % (username, ))
                return False

        try:
            string_to_sign = _string_to_sign(username, req)
        except KeyError:
            return False

        try:
            timestamp = int(req.headers['x-nimbus-io-timestamp'])
        except (TypeError, ValueError):
            return False

        if abs(time.time() - timestamp) > 600:
            return False

        try:
            signature = a2b_hex(signature)
        except (TypeError, ValueError):
            return False

        expected = hmac.new(
            customer_key.key, string_to_sign, hashlib.sha256
        ).digest()

        if not sec_str_eq(signature, expected):
            self._log.debug("signature comparison failed %r" % (username, ))
            return False

        # cache the authentication results with an expiration time
        self._customer_key_cache[username] = (
            customer_key, 
            time.time() + _customer_key_cache_expiration_interval,
        )

        return True

