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
from cStringIO import StringIO
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
    assert type(body) == list, body
    assert len(body) == 1, body

    _archive(config, message, StringIO(body[0]), send_queue)

def archive_file(config, message, _body, send_queue):
    """
    archive a file of data passed as an argument
    """
    _archive(config, message, open(message["path"]), send_queue)

def _archive(config, message, file_object, send_queue):
    log = logging.getLogger("_archive")

    connection = httplib.HTTPConnection(config["BaseAddress"])

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : "sending request",
        "error-message" : None,
        "completed"     : False,        
    }
    send_queue.put((status_message, None, ))

    method = "POST"
    key = (
        message["key"][1:] if message["key"][0] == os.sep else message["key"]
    )
 
    uri = os.path.join(os.sep, "data", key)
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

    log.info("uri = '%s'" % (uri, ))
    connection.request(method, uri, body=file_object, headers=headers)

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
        
