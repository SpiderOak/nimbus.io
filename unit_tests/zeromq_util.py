# -*- coding: utf-8 -*-
"""
zeromq_util.py

Utility functions for testing zeromq servers
"""
from collections import deque
from threading import Event
import time

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.xreq_client import XREQClient

def send_request_and_get_reply(address, request, data=None):
    reply, _ = send_request_and_get_reply_and_data(address, request, data)
    return reply

def send_request_and_get_reply_and_data(address, request, data=None):
    context = zmq.Context()
    pollster = ZeroMQPollster()
    receive_queue = deque()
    xreq_client = XREQClient(
        context,
        address,
        receive_queue
    )
    xreq_client.register(pollster)

    xreq_client.queue_message_for_send(request, data)

    halt_event = Event()
    retry_count = 0
    reply = None
    data = None
    while retry_count < 10:
        pollster.run(halt_event)
        try:
            reply, data = receive_queue.popleft()
        except IndexError:
            retry_count += 1
            time.sleep(1.0)
            continue
        else:
            break

    xreq_client.close()
    context.term()
    return reply, data

