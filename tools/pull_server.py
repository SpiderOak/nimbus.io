# -*- coding: utf-8 -*-
"""
pull_server.py

a class that manages a zeromq PULL socket as a server,
to multiple PUSH clients
"""
import logging

import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

class PULLServer(object):
    """
    context
        zeromq context

    address
        the address our socket binds to. Sent to the remote server in the 
        initial handshake, by the resilient client.

    receve_queue
        The queue (deque) where we put incoming messages. This is shared with
        the DequeDispatcher

    Each process maintains a single PULL_ socket for all of its resilient 
    clients. This class wraps that socket.

    .. _PULL: http://api.zeromq.org/2-1:zmq-socket#toc13
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
        """
        register this socket with the zeromq pollster
        """
        pollster.register_read(self._pull_socket, self._pollster_callback)

    def unregister(self, pollster):
        """
        unregister this socket from the zeromq pollster
        """
        pollster.unregister(self._pull_socket)

    def close(self):
        """
        close the socket
        """
        self._pull_socket.close()

    def _pollster_callback(self, _active_socket, readable, writable):
        message = self._receive_message()      
        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return None
        self._receive_queue.append((message.control, message.body, ))
            
    def _receive_message(self):
        try:
            control = self._pull_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        body = []
        while self._pull_socket.rcvmore:
            body.append(self._pull_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return message_format(ident=None, control=control, body=body)

