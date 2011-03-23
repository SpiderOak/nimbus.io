# -*- coding: utf-8 -*-
"""
xreq_client_task.py

a class that manages a zeromq XREQ socket as a client,
to an XREP server
"""
from collections import deque, namedtuple
import logging

import zmq

# our internal message format
_message_format = namedtuple("Message", "control body")

class XREQClient(object):
    """
    a class that manages a zeromq XREQ socket as a client
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("XREQClient-%s" % (address, ))

        self._xreq_socket = context.socket(zmq.XREQ)
        self._log.debug("connecting")
        self._xreq_socket.connect(address)

        self._send_queue = deque()
        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read_or_write(
            self._xreq_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._xreq_socket)

    def close(self):
        self._xreq_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        self._send_queue.append(
            _message_format(control=message_control, body=data)
        )

    def _pollster_callback(self, _active_socket, readable, writable):

        # push our output first
        while writable:
            try:
                message = self._send_queue.popleft()
            except IndexError:
                break
            self._send_message(message)

        # if we have input, read it and queue it
        if readable:
            message = self._receive_message()      
            # if we get None, that means the socket would have blocked
            # go back and wait for more
            if message is None:
                return None
            self._receive_queue.append((message.control, message.body, ))
            
    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))
        if message.body is not None:
            self._xreq_socket.send_json(message.control, zmq.SNDMORE)
            self._xreq_socket.send(message.body, copy=False)
        else:
            self._xreq_socket.send_json(message.control)

    def _receive_message(self):
        try:
            control = self._xreq_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        if self._xreq_socket.rcvmore():
            body = self._xreq_socket.recv()
        else:
            body = None

        return _message_format(control=control, body=body)

