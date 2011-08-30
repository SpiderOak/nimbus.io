# -*- coding: utf-8 -*-
"""
sql_authenticator.py

Authenticates requests
"""
from binascii import a2b_hex
from collections import namedtuple
import hashlib
import hmac
import logging
import time

from diyapi_tools.LRUCache import LRUCache
from diyapi_web_server.util import sec_str_eq

_max_auth_cache_size = 10000
_auth_cache_expiration_interval = 60.0 * 60.0 # 1 hour
_user_info_template = namedtuple("UserInfo", ["key_id", "key", "avatar_id"])

def _string_to_sign(req):
    return '\n'.join((
        req.diy_username,
        req.method,
        req.headers['x-nimbus-io-timestamp'],
        req.path_qs,
    ))

class SqlAuthenticator(object):
    def __init__(self, connection):
        self._log = logging.getLogger("SqlAuthenticator")
        self.connection = connection
        self._auth_cache = LRUCache(_max_auth_cache_size)

    def _get_user_info_from_database(self, username):
        cur = self.connection.cursor()
        cur.execute("""
            select diy_user_key.key_id, diy_key.key, diy_user.avatar_id 
            from diy_user inner join diy_user_key using (user_id) 
            inner join diy_key using (key_id) where diy_user.username = %s""", 
            [username, ]
        )
        result = cur.fetchall()
        cur.close()
        return [_user_info_template._make(row) for row in result]

    def authenticate(self, req):
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
    
        user_info = None
        # test for cached authentification
        if req.diy_username in self._auth_cache:
            cached_user_info, auth_expiration_time = \
                    self._auth_cache[req.diy_username]
            if time.time() > auth_expiration_time:
                del self._auth_cache[req.diy_username]
            else:
                user_info = cached_user_info

        if user_info is None:
            # no cached avatar, or cache has expired
            try:
                user_info_rows = self._get_user_info_from_database(
                    req.diy_username
                )
            except TypeError:
                return False
            for user_info_row in user_info_rows:
                if user_info_row.key_id == key_id:
                    user_info = user_info_row
                    break

        if user_info is None:
            self._log.debug("unknown user %r" % (req.diy_username, ))
            return False

        try:
            string_to_sign = _string_to_sign(req)
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
            user_info.key, string_to_sign, hashlib.sha256
        ).digest()

        if not sec_str_eq(signature, expected):
            self._log.debug("signature comparison failed %r %r" % (
                req.diy_username, user_info,
            ))
            return False

        req.avatar_id = user_info.avatar_id
        req.key_id = key_id

        # cache the authentication results with an expiration time
        self._auth_cache[req.diy_username] = (
            user_info, 
            time.time() + _auth_cache_expiration_interval,
        )

        return True
