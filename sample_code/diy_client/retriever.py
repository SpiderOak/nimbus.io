# -*- coding: utf-8 -*-
"""
retriever.py

retrieve one file
"""
import httplib
import logging

from sample_code.diy_client.http_util import compute_authentication_string, \
        compute_uri, \
        current_timestamp

_read_buffer_size = 64 * 1024

def retrieve_file(config, message, _body, send_queue):
    """
    retrieve a file
    """
    file_object = open(message["dest-path"], "w")
    _retrieve(config, message, file_object, send_queue)
    file_object.close()

def _retrieve(config, message, dest_file, send_queue):
    log = logging.getLogger("_retrieve")

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

    while True:
        data = response.read(_read_buffer_size)
        if len(data) == 0:
            break
        dest_file.write(data)

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
        
