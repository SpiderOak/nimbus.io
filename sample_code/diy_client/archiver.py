# -*- coding: utf-8 -*-
"""
archiver.py

archive one file
"""
import logging

from sample_code.diy_client.http_util import compute_uri
from sample_code.diy_client.http_connection import HTTPConnection, \
        HTTPRequestError

def archive_blob(config, message, body, send_queue):
    """
    archive a blob of data passed as an argument
    """
    assert type(body) == list, body
    assert len(body) == 1, body

    _archive(config, message, body[0], send_queue)

def archive_file(config, message, _body, send_queue):
    """
    archive a file of data passed as an argument
    """
    file_object = open(message["path"], "r")
    _archive(config, message, file_object, send_queue)
    file_object.close()

def _archive(config, message, body, send_queue):
    """
    If the body argument is present, it should be a string of data to send 
    after the headers are finished. Alternatively, it may be an open file 
    object, in which case the contents of the file is sent; 
    this file object should support fileno() and read() methods. 
    """
    log = logging.getLogger("_archive")

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

    method = "POST"
    uri = compute_uri(message["key"]) 

    log.info("requesting %s" % (uri, ))
    try:
        response = connection.request(method, uri, body=body)
    except HTTPRequestError, instance:
        log.error(str(instance))
        status_message["status"] = "error"
        status_message["error-message"] = str(instance)
        connection.close()
        send_queue.put((status_message, None, ))
        return
    else:
        log.info("reading response")
        response.read()
    finally:
        log.info("closing connection")
        connection.close()
    
    log.info("archive complete")
    status_message["status"] = "OK"
    send_queue.put((status_message, None, ))
        
