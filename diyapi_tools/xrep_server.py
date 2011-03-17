# -*- coding: utf-8 -*-
"""
xrep_server_task.py

a class that manages a zeromq XREP socket as a server,
"""
from collections import deque, namedtuple
import logging

import zmq

from diyapi_tools.zeromq_util import prepare_ipc_path

# our internal message format
_message_format = namedtuple("Message", "ident control body")

class XREPServer(object):
    """
    a class that manages a zeromq XREP socket as a server
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("XREPServer-%s" % (address, ))

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._xrep_socket = context.socket(zmq.XREP)
        self._log.debug("binding")
        self._xrep_socket.bind(address)

        self._send_queue = deque()
        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read_or_write(
            self._xrep_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._xrep_socket)

    def close(self):
        self._xrep_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        message_ident = message_control.pop("xrep-ident")
        self._send_queue.append(
            _message_format(
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
                
            # stuff the XREP ident into the message, we'll want it back
            # if the caller tries to send something
            message.control["xrep-ident"] = message.ident
            self._receive_queue.append(
                (message.control, message.body, )
            )

    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))
        self._xrep_socket.send(message.ident, zmq.SNDMORE)
        if message.body is not None:
            self._xrep_socket.send_json(message.control, zmq.SNDMORE)
            self._xrep_socket.send(message.body, copy=False)
        else:
            self._xrep_socket.send_json(message.control)

    def _receive_message(self):
        try:
            ident = self._xrep_socket.recv(zmq.NOBLOCK)        
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        assert self._xrep_socket.rcvmore(), \
            "Unexpected missing message control part."
        control = self._xrep_socket.recv_json()

        if self._xrep_socket.rcvmore():
            body = self._xrep_socket.recv()
        else:
            body = None

        return _message_format(ident=ident, control=control, body=body)

