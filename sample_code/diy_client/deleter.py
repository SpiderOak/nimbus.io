# -*- coding: utf-8 -*-
"""
deleter.py

delete one file
"""
import httplib
import logging

from sample_code.diy_client.http_util import compute_authentication_string, \
        compute_uri, \
        current_timestamp

def delete_file(config, message, _body, send_queue):
    """
    delete a file
    """
    _delete(config, message, None, send_queue)

def _delete(config, message, _body, send_queue):
    log = logging.getLogger("_delete")

    connection = httplib.HTTPConnection(config["BaseAddress"])

    status_message = {
        "message-type"  : message["client-topic"],
        "status"        : "sending request",
        "error-message" : None,
        "completed"     : False,        
    }
    send_queue.put((status_message, None, ))

    method = "DELETE"
    timestamp = current_timestamp()
    uri = compute_uri(message["key"]) 
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
    response.read()
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
        
