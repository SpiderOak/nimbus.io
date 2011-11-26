# -*- coding: utf-8 -*-
"""
router_server.py

a class that manages a zeromq ROUTER (aka XREP) socket as a server,
"""
from collections import deque
import logging

import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

class RouterServer(object):
    """
    a class that manages a zeromq ROUTER (aka XREP) socket as a server
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("RouterServer-%s" % (address, ))

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._router_socket = context.socket(zmq.XREP)
        self._router_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("binding")
        self._router_socket.bind(address)

        self._send_queue = deque()
        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read_or_write(
            self._router_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._router_socket)

    def close(self):
        self._router_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        message_ident = message_control.pop("router-ident")
        self._send_queue.append(
            message_format(
                ident=message_ident, control=message_control, body=data
            )
        )

    def _pollster_callback(self, _active_socket, readable, writable):

        # push our output first, no point in taking on new work
        # when we haven't sent our old stuff
        while writable:
            try:
                message = self._send_queue.popleft()
            except IndexError:
                break
            self._send_message(message)

        # if we have input, read it and handle it
        if readable:
            message = self._receive_message()      
            # if we get None, that means the socket would have blocked
            # go back and wait for more
            if message is None:
                return
                
            # stuff the ident into the message, we'll want it back
            # if the caller tries to send something
            message.control["router-ident"] = message.ident
            self._receive_queue.append(
                (message.control, message.body, )
            )

    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))
        self._router_socket.send(message.ident, zmq.SNDMORE)

        # don't send a zero size body 
        if type(message.body) not in [list, tuple, type(None), ]:
            if len(message.body) == 0:
                message = message._replace(body=None)
            else:
                message = message._replace(body=[message.body, ])

        if message.body is None:
            self._router_socket.send_json(message.control)
        else:
            self._router_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._router_socket.send(segment, zmq.SNDMORE)
            self._router_socket.send(message.body[-1])

    def _receive_message(self):
        try:
            ident = self._router_socket.recv(zmq.NOBLOCK)        
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        assert self._router_socket.rcvmore(), \
            "Unexpected missing message control part."
        control = self._router_socket.recv_json()

        body = []
        while self._router_socket.rcvmore():
            body.append(self._router_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return message_format(ident=ident, control=control, body=body)

