# -*- coding: utf-8 -*-
"""
dealer_client.py

a class that manages a zeromq DEALER (aka XREQ) socket as a client,
to a ROUTER (aka XREP) server
"""
from collections import deque
import logging

import zmq

from tools.data_definitions import message_format

class DealerClient(object):
    """
    a class that manages a zeromq DEALER (aka XREQ) socket as a client
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("DealerClient-%s" % (address, ))

        self._dealer_socket = context.socket(zmq.XREQ)
        self._dealer_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting")
        self._dealer_socket.connect(address)

        self._send_queue = deque()
        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read_or_write(
            self._dealer_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._dealer_socket)

    def close(self):
        self._dealer_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        self._send_queue.append(
            message_format(ident=None, control=message_control, body=data)
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
        self._log.debug("sending message: %s" % (message.control, ))

        # don't send a zero size body 
        if type(message.body) not in [list, tuple, type(None), ]:
            if len(message.body) == 0:
                message = message._replace(body=None)
            else:
                message = message._replace(body=[message.body, ])

        if message.body is None:
            self._dealer_socket.send_json(message.control)
        else:
            self._dealer_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._dealer_socket.send(segment, zmq.SNDMORE)
            self._dealer_socket.send(message.body[-1])

    def _receive_message(self):
        try:
            control = self._dealer_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        body = []
        while self._dealer_socket.rcvmore:
            body.append(self._dealer_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return message_format(ident=None, control=control, body=body)

