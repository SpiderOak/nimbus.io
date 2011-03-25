# -*- coding: utf-8 -*-
"""
pull_server.py

a class that manages a zeromq PULL socket as a server,
to multiple PUSH clients
"""
from collections import namedtuple
import logging

import zmq

from diyapi_tools.zeromq_util import prepare_ipc_path

_message_format = namedtuple("Message", "control body")

class PULLServer(object):
    """
    a class that manages a zeromq PULL socket as a server,
    to multiple PUSH clients
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("PULLServer-%s" % (address, ))

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._pull_socket = context.socket(zmq.PULL)
        self._log.debug("binding")
        self._pull_socket.bind(address)

        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read(self._pull_socket, self._pollster_callback)

    def unregister(self, pollster):
        pollster.unregister(self._pull_socket)

    def close(self):
        self._pull_socket.close()

    def _pollster_callback(self, _active_socket, readable, writable):
        message = self._receive_message()      
        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return None
        self._receive_queue.append(message)
            
    def _receive_message(self):
        try:
            control = self._pull_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        if self._pull_socket.rcvmore():
            body = self._pull_socket.recv()
        else:
            body = None

        return _message_format(control=control, body=body)

