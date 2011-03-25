# -*- coding: utf-8 -*-
"""
push_client.py

a class that manages a zeromq PUSH socket as a client,
The purpose is to have multiple clients pushing to a single PULL server
"""
from collections import deque, namedtuple
import logging

import zmq

_message_format = namedtuple("Message", "control body")

class PUSHClient(object):
    """
    a class that manages a zeromq PUSH socket as a client,
    The purpose is to have multiple clients pushing to a single PULL server
    """
    def __init__(self, context, address):
        self._log = logging.getLogger("PUSHClient-%s" % (address, ))

        self._push_socket = context.socket(zmq.PUSH)
        self._log.debug("connecting")
        self._push_socket.connect(address)

        self._send_queue = deque()

    def register(self, pollster):
        pollster.register_write(
            self._push_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._push_socket)

    def close(self):
        self._push_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        self._send_queue.append(
            _message_format(control=message_control, body=data)
        )

    def _pollster_callback(self, _active_socket, readable, writable):
        while writable:
            try:
                message = self._send_queue.popleft()
            except IndexError:
                break
            self._send_message(message)

    def _send_message(self, message):
        self._log.info("sending message: %s" % (
            message.control,  
        ))
        if message.body is not None:
            self._push_socket.send_json(message.control, zmq.SNDMORE)
            self._push_socket.send(message.body, copy=False)
        else:
            self._push_socket.send_json(message.control)

