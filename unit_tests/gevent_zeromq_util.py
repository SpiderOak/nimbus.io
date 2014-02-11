# -*- coding: utf-8 -*-
"""
gevent_zeromq_util.py

Utility functions for testing zeromq servers
using the same gevent driven objects used by the web server
"""
import logging

from gevent import monkey
monkey.patch_all()

try:
    import gevent_zeromq
    gevent_zeromq.monkey_patch()
except ImportError:
    import zmq

import gevent
from gevent_zeromq import zmq

from tools.greenlet_resilient_client import GreenletResilientClient
from tools.greenlet_pull_server import GreenletPULLServer
from tools.deliverator import Deliverator
from tools.greenlet_push_client import GreenletPUSHClient

class UtilError(Exception):
    pass

def send_request_and_get_reply(
    server_node_name,
    server_address, 
    client_tag, 
    client_address, 
    request, 
    data=None
):
    reply, _ = send_request_and_get_reply_and_data(
        server_node_name,
        server_address, 
        client_tag, 
        client_address, 
        request, 
        data
    )
    return reply

def send_request_and_get_reply_and_data(
    server_node_name,
    server_address, 
    client_tag, 
    client_address, 
    request, 
    data=None
):
    log = logging.getLogger("send_request_and_get_reply_and_data")
    context = zmq.Context()
    deliverator = Deliverator()

    pull_server = GreenletPULLServer(
        context, 
        client_address,
        deliverator
    )
    pull_server.start()

    resilient_client = GreenletResilientClient(
        context,
        server_node_name,
        server_address,
        client_tag,
        client_address,
        deliverator,
    )
    resilient_client.start()

    # loop until the resilient client connects
    test_status_count = 0
    while True:
        status_name, _, __ = resilient_client.test_current_status()
        if status_name == "connected":
            break
        test_status_count += 1
        if test_status_count > 5:
            log.error("too many status retries")
            raise UtilError("too many status retries")

        log.warn("status retry delay")
        gevent.sleep(10.0)

    delivery_channel = resilient_client.queue_message_for_send(request, data)
    reply, data = delivery_channel.get()

    pull_server.kill()
    resilient_client.kill()
    pull_server.join()
    resilient_client.join()

    context.term()
    return reply, data

def send_to_pipeline(node_name, address, message_generator):
    context = zmq.Context()
    push_client = GreenletPUSHClient(
        context,
        node_name,
        address,
    )

    for message, data in message_generator:    
        push_client.send(message, data)
    gevent.sleep(2.0)

    push_client.close()
    context.term()

