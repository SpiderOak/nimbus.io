# -*- coding: utf-8 -*-
"""
authenticator.py

Authenticates requestuests
"""
from binascii import a2b_hex
from collections import namedtuple
import hashlib
import hmac
import logging
import time
import urllib

from web_server.util import sec_str_eq

_customer_key_template = namedtuple("CustomerKey", ["key_id", "key"])

def _get_customer_key(connection, username, key_id):
    """
    retrieve a specific key for the customer
    """
    # we could just select on id, but we want to make sure this key
    # belongs to this user
    cursor = connection.cursor()
    cursor.execute("""
        select key from nimbusio_central.customer_key
        where customer_id = (select id from nimbusio_central.customer
                             where username = %s)
        and id = %s
        """, [username, key_id, ])
    result = cursor.fetchone()
    cursor.close()

    if result is None:
        return None

    ( key, ) = result
    return _customer_key_template(key_id=key_id, key=key)

def _string_to_sign(username, request):
    # we want the path and the query string
    if "?" in request.url:
        _, qs = request.url.split("?")
        path_qs = "?".join([request.path, qs])
    else:
        path_qs = request.path
    return '\n'.join([username,
                      request.method,
                      request.headers['x-nimbus-io-timestamp'],
                      urllib.unquote_plus(path_qs), ])

def authenticate(connection, username, request):
    """
    authenticate user request
    """
    log = logging.getLogger("authenticate")
    log.debug("{0} {1}".format(username, request.url))
    try:
        auth_type, auth_string = request.headers["Authorization"].split()
    except Exception, instance:
        log.error("invalid request.authorization {0} {1}".format(
            instance, request.authorization))
        return False

    if auth_type != 'NIMBUS.IO':
        log.error("unknown auth_type %r" % (auth_type, ))
        return False

    try:
        key_id, signature = auth_string.split(':', 1)
    except Exception, instance:
        log.error("invalid auth_string {0} {1}".format(
            instance, auth_string))
        return False

    try:
        key_id = int(key_id)
    except Exception, instance:
        log.error("invalid key_id {0} {1}".format(
            instance, key_id))
        return False

    customer_key = _get_customer_key(connection, username, key_id)
    if customer_key is None:
        log.error("unknown user %r" % (username, ))
        return False

    try:
        string_to_sign = _string_to_sign(username, request)
    except Exception, instance:
        log.error("_string_to_sign failed {0} {1} {2}".format(
            instance, username, request))
        return False

    try:
        timestamp = int(request.headers['x-nimbus-io-timestamp'])
    except Exception, instance:
        log.error("invalid x-nimbus-io-timestamp {0} {1}".format(
            instance, request.headers))
        return False

    # The timestamp must agree within 10 minutes of that on the server
    time_delta = abs(time.time() - timestamp)
    if time_delta > 600:
        log.error("timestamp out of range {0} {1}".format(
            timestamp, time_delta))
        return False

    try:
        signature = a2b_hex(signature)
    except Exception, instance:
        log.error("a2b_hex(signature) failed {0} {1}".format(
            timestamp, instance))
        return False

    expected = hmac.new(
        customer_key.key, string_to_sign, hashlib.sha256
    ).digest()

    if not sec_str_eq(signature, expected):
        log.error("signature comparison failed %r %r" % (
            username, string_to_sign
        ))
        return False

    return True

