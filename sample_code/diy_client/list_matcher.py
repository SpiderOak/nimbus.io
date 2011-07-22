# -*- coding: utf-8 -*-
"""
list_matcher.py

request list match
"""
import json
import os.path
import logging

from sample_code.diy_client.http_util import compute_uri
from sample_code.diy_client.http_connection import HTTPConnection, \
        HTTPRequestError

def list_match(config, message, _body, send_queue):
    """
    list match
    """
    log = logging.getLogger("list_match")

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : None,
        "error-message" : None,
        "completed"     : True,        
    }

    connection = HTTPConnection(
        config["BaseAddress"],
        config["Username"], 
        config["AuthKey"],
        config["AuthKeyId"]
    )

    method = "GET"
    uri = compute_uri(message["prefix"], action="listmatch")

    try:
        response = connection.request(method, uri)
    except HTTPRequestError, instance:
        status_message["status"] = "error"
        status_message["error-message"] = str(instance)
        connection.close()
        send_queue.put((status_message, None, ))
        return
    else:
        data = response.read()
    finally:
        connection.close()

    status_message["status"] = "OK"
    log.info("listmatch successful %s" % (data, ))
    send_queue.put((status_message, [data, ], ))
        
