# -*- coding: utf-8 -*-
"""
http_util.py

utility functions for connecting wiht DIY via HTTP
"""
import hashlib
import hmac
import os
import time

def compute_authentication_string(
    user_name, auth_key, auth_key_id, method, timestamp
):
    """
    Compute the authentication hmac sent to the server
    """
    message = "\n".join([user_name, method, str(timestamp)])
    hmac_object = hmac.new(
        auth_key,
        message,
        hashlib.sha256
    )
    return "DIYAPI %s:%s" % (auth_key_id, hmac_object.hexdigest(), )

def compute_uri(key):
    """
    Create the REST URI sent to the server
    """
    work_key = (key[1:] if key[0] == os.sep else key)
    return os.path.join(os.sep, "data", work_key)

def current_timestamp():
    """
    return the current time as an integer
    """
    return int(time.time())


