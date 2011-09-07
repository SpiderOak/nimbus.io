# -*- coding: utf-8 -*-
"""
gevent_zeromq_util.py

Utility functions for testing zeromq servers
using the same gevent driven objects used by the web server
"""
import logging

import gevent
from gevent_zeromq import zmq

from tools.greenlet_zeromq_pollster import GreenletZeroMQPollster
from tools.greenlet_resilient_client import GreenletResilientClient
from tools.greenlet_pull_server import GreenletPULLServer
from tools.deliverator import Deliverator
from tools.greenlet_push_client import GreenletPUSHClient

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
    context = zmq.context.Context()
    pollster = GreenletZeroMQPollster()
    deliverator = Deliverator()

    pull_server = GreenletPULLServer(
        context, 
        client_address,
        deliverator
    )
    pull_server.register(pollster)

    resilient_client = GreenletResilientClient(
        context,
        pollster,
        server_node_name,
        server_address,
        client_tag,
        client_address,
        deliverator,
    )

    pollster.start()

    delivery_channel = resilient_client.queue_message_for_send(request, data)
    reply, data = delivery_channel.get()

    pollster.kill()
    pollster.join(timeout=3.0)

    pull_server.close()
    resilient_client.close()

    context.term()
    return reply, data

def send_to_pipeline(node_name, address, message_generator):
    context = zmq.context.Context()
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

