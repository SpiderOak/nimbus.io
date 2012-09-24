# -*- coding: utf-8 -*-
"""
authenticator.py

Authenticates requestuests
"""
from binascii import a2b_hex
import hashlib
import hmac
import logging
import time
import urllib

from web_public_reader.util import sec_str_eq

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

def authenticate(customer_key_lookup, username, request):
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
        return None

    if auth_type != 'NIMBUS.IO':
        log.error("unknown auth_type %r" % (auth_type, ))
        return None

    try:
        key_id, signature = auth_string.split(':', 1)
    except Exception, instance:
        log.error("invalid auth_string {0} {1}".format(
            instance, auth_string))
        return None

    try:
        key_id = int(key_id)
    except Exception, instance:
        log.error("invalid key_id {0} {1}".format(
            instance, key_id))
        return None

    customer_key_row = customer_key_lookup.get(key_id)
    if customer_key_row is None:
        log.error("unknown customer key {0}".format(key_id))
        return None

    try:
        string_to_sign = _string_to_sign(username, request)
    except Exception, instance:
        log.error("_string_to_sign failed {0} {1} {2}".format(
            instance, username, request))
        return None

    try:
        timestamp = int(request.headers['x-nimbus-io-timestamp'])
    except Exception, instance:
        log.error("invalid x-nimbus-io-timestamp {0} {1}".format(
            instance, request.headers))
        return None

    # The timestamp must agree within 10 minutes of that on the server
    time_delta = abs(time.time() - timestamp)
    if time_delta > 600:
        log.error("timestamp out of range {0} {1}".format(
            timestamp, time_delta))
        return None

    try:
        signature = a2b_hex(signature)
    except Exception, instance:
        log.error("a2b_hex(signature) failed {0} {1}".format(
            timestamp, instance))
        return None

    expected = hmac.new(str(customer_key_row["key"]), 
                        string_to_sign, 
                        hashlib.sha256).digest()

    if not sec_str_eq(signature, expected):
        log.error("signature comparison failed %r %r" % (
            username, string_to_sign
        ))
        return None

    # Ticket #49 collection manager allows authenticated users to set 
    # versioning property on collections they don't own 
    # return customer_id so we can check ownership
    return customer_key_row["customer_id"]

