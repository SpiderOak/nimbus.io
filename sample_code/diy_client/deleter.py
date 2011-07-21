# -*- coding: utf-8 -*-
"""
deleter.py

delete one file
"""
from sample_code.diy_client.http_util import compute_uri
from sample_code.diy_client.http_connection import HTTPConnection, \
        HTTPRequestError

def delete_file(config, message, _body, send_queue):
    """
    delete a file
    """
    status_message = {
        "message-type"  : message["client-topic"],
        "key"           : message["key"], 
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

    method = "DELETE"
    uri = compute_uri(message["key"]) 

    try:
        response = connection.request(method, uri)
    except HTTPRequestError, instance:
        status_message["status"] = "error"
        status_message["error-message"] = str(instance)
        connection.close()
        send_queue.put((status_message, None, ))
        return
    else:
        response.read()
    finally:
        connection.close()

    status_message["status"] = "OK"
    send_queue.put((status_message, None, ))
        
