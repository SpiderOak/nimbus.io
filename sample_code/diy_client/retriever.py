# -*- coding: utf-8 -*-
"""
retriever.py

retrieve one file
"""
import logging

from sample_code.diy_client.http_util import compute_uri
from sample_code.diy_client.http_connection import HTTPConnection, \
        HTTPRequestError

_read_buffer_size = 64 * 1024

def retrieve_blob(config, message, _dest_file, send_queue):
    log = logging.getLogger("_retrieve")

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

    method = "GET"
    uri = compute_uri(message["key"]) 

    log.info("requesting %s" % (uri, ))
    body_list = list()
    try:
        response = connection.request(method, uri)
    except HTTPRequestError, instance:
        log.error(str(instance))
        status_message["status"] = "error"
        status_message["error-message"] = str(instance)
        connection.close()
        send_queue.put((status_message, None, ))
        return
    else:
        while True:
            data = response.read(_read_buffer_size)
            if len(data) == 0:
                break
            body_list.append(data)
    finally:
        connection.close()
    
    log.info("retrieve complete")
    status_message["status"] = "OK"
    send_queue.put((status_message, body_list, ))

def retrieve_file(config, message, dest_file, send_queue):
    log = logging.getLogger("_retrieve")

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

    method = "GET"
    uri = compute_uri(message["key"]) 

    log.info("requesting %s" % (uri, ))
    file_object = open(message["dest-path"], "w")
    try:
        response = connection.request(method, uri)
    except HTTPRequestError, instance:
        log.error(str(instance))
        status_message["status"] = "error"
        status_message["error-message"] = str(instance)
        connection.close()
        send_queue.put((status_message, None, ))
        return
    else:
        while True:
            data = response.read(_read_buffer_size)
            if len(data) == 0:
                break
            dest_file.write(data)
    finally:
        connection.close()
        file_object.close()
    
    log.info("retrieve complete")
    status_message["status"] = "OK"
    send_queue.put((status_message, None, ))

