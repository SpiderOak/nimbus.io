# -*- coding: utf-8 -*-
"""
space_usage_requestor.py

request space usage
"""
import httplib
import os.path
import logging

from sample_code.diy_client.http_util import compute_authentication_string, \
        current_timestamp

def request_space_usage(config, message, _body, send_queue):
    """
    request space usage
    """
    log = logging.getLogger("request_space_usage")

    connection = httplib.HTTPConnection(config["BaseAddress"])

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : "sending request",
        "error-message" : None,
        "completed"     : False,        
    }
    send_queue.put((status_message, None, ))

    method = "GET"
    timestamp = current_timestamp()
    uri = os.path.join(os.sep, "usage") 
    authentication_string = compute_authentication_string(
        config["Username"], 
        config["AuthKey"],
        config["AuthKeyId"],
        method, 
        timestamp
    )

    headers = {
        "Authorization"         : authentication_string,
        "X-DIYAPI-Timestamp"    : str(timestamp),
        "agent"                 : 'diy-tool/1.0'
    }

    log.info("uri = '%s'" % (uri, ))
    connection.request(method, uri, body=None, headers=headers)

    response = connection.getresponse()
    data = response.read()
    connection.close()

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : None,
        "error-message" : None,
        "space-usage"   : None,
        "completed"     : True,        
    }

    if response.status == httplib.OK:
        status_message["status"] = "OK"
        status_message["space-usage"] = data
        log.info("space usage successful %s" % (data, ))
    else:
        message = "request failed %s %s" % (response.status, response.reason, ) 
        log.warn(message)
        status_message["status"] = "error"
        status_message["error-message"] = message

    send_queue.put((status_message, None, ))
        
