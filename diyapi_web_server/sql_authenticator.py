# -*- coding: utf-8 -*-
"""
sql_authenticator.py

Authenticates requests
"""
import time
import hmac
import hashlib
from binascii import a2b_hex
from diyapi_tools.LRUCache import LRUCache

_max_auth_cache_size = 100
_auth_cache_expiration_interval = 60.0 * 60.0 # 1 hour

class SqlAuthenticator(object):
    def __init__(self, connection):
        self.connection = connection
        self._auth_cache = LRUCache(_max_auth_cache_size)

    def _string_to_sign(self, req):
        return '\n'.join((
            req.diy_username,
            req.method,
            req.headers['x-diyapi-timestamp'],
        ))

    def _get_key_id_and_avatar_id(self, username):
        cur = self.connection.cursor()
        cur.execute('select key_id, avatar_id '
                    'from diy_user '
                    'join diy_user_key using (user_id) '
                    'where username=%s',
                    [username])
        row = cur.fetchone()
        if row:
            return tuple(row)

    def _get_key(self, key_id):
        cur = self.connection.cursor()
        cur.execute('select key from diy_key where key_id=%s',
                    [key_id])
        row = cur.fetchone()
        if row:
            return row[0]

    def authenticate(self, req):
        try:
            auth_type, auth_string = req.authorization
        except TypeError:
            return False
        if auth_type != 'DIYAPI':
            return False
        try:
            key_id, signature = auth_string.split(':', 1)
        except TypeError:
            return False
        try:
            key_id = int(key_id)
        except (TypeError, ValueError):
            return False
    
        # test for cached authentification
        if req.diy_username in self._auth_cache:
            auth_key_id, auth_avatar_id, auth_expiration_time = \
                    self._auth_cache[req.diy_username]
            if time.time() > auth_expiration_time:
                del self._auth_cache[req.diy_username]
            elif key_id == auth_key_id:
                req.avatar_id = auth_avatar_id
                req.key_id = auth_key_id
                return True

        try:
            db_key_id, avatar_id = self._get_key_id_and_avatar_id(
                req.diy_username)
        except TypeError:
            return False
        if key_id != db_key_id:
            return False
        key = self._get_key(key_id)
        if not key:
            return False
        try:
            string_to_sign = self._string_to_sign(req)
        except KeyError:
            return False
        try:
            timestamp = int(req.headers['x-diyapi-timestamp'])
        except (TypeError, ValueError):
            return False
        if abs(time.time() - timestamp) > 600:
            return False
        try:
            signature = a2b_hex(signature)
        except (TypeError, ValueError):
            return False
        expected = hmac.new(key, string_to_sign, hashlib.sha256).digest()
        if signature != expected:
            return False

        req.avatar_id = int(avatar_id)
        req.key_id = int(key_id)

        # cache the authentication results with an expiration time
        self._auth_cache[req.diy_username] = (
            req.key_id, 
            req.avatar_id, 
            time.time() + _auth_cache_expiration_interval,
        )

        return True
