# -*- coding: utf-8 -*-
"""
archiver.py

archive one file
"""
import hashlib
import hmac
import httplib
import logging
import os
import os.path
import time

def compute_authentication_string(config, method, timestamp):
    message = "\n".join([config["Username"], method, str(timestamp)])
    hmac_object = hmac.new(
        config["AuthKey"],
        message,
        hashlib.sha256
    )
    return "DIYAPI %s:%s" % (config["AuthKeyId"], hmac_object.hexdigest(), )

def archive_blob(config, message, body, send_queue):
    """
    archive a blob of data passed as an argument
    """
    log = logging.getLogger("archive_blob")

    assert type(body) == list, body
    assert len(body) == 1, body

    connection = httplib.HTTPConnection(config["BaseAddress"])

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : "sending request",
        "error-message" : None,
        "completed"     : False,        
    }
    send_queue.put((status_message, None, ))

    method = "POST"
    uri = os.path.join(os.sep, "data", message["key"])
    timestamp = int(time.time())
        
    authentification_string = compute_authentication_string(
        config, 
        method, 
        timestamp
    )

    headers = {
        "Authorization"         : authentification_string,
        "X-DIYAPI-Timestamp"    : str(timestamp),
        "agent"                 : 'diy-tool/1.0'
    }

    connection.request(method, uri, body=body[0], headers=headers)

    response = connection.getresponse()
    connection.close()

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : None,
        "error-message" : None,
        "completed"     : True,        
    }

    if response.status == httplib.OK:
        status_message["status"] = "OK"
        log.info("archvie successful")
    else:
        message = "request failed %s %s" % (response.status, response.reason, ) 
        log.warn(message)
        status_message["status"] = "error"
        status_message["error-message"] = message

    send_queue.put((status_message, None, ))
        
