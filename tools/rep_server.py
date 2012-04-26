# -*- coding: utf-8 -*-
"""
rep_server.py

a class that manages a zeromq REP socket as a server,
"""
import logging

import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

class REPServer(object):
    """
    a class that manages a zeromq REP socket as a server
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("REPServer-{0}".format(address))

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._rep_socket = context.socket(zmq.REP)
        self._rep_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("binding")
        self._rep_socket.bind(address)

        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read(self._rep_socket, self._pollster_callback)

    def unregister(self, pollster):
        pollster.unregister(self._rep_socket)

    def close(self):
        self._rep_socket.close()

    def send_reply(self, message_control, data=None):
        message = message_format(ident=None, 
                                 control=message_control, 
                                 body=data)
        self._send_message(message)

    def _pollster_callback(self, _active_socket, readable, writable):
        """
        this socket wants a rigid request-reply sequence,
        a read must always be followed by a write
        """
        assert readable

        message = self._receive_message()      
        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return
        self._receive_queue.append((message.control, message.body, ))
                
    def _send_message(self, message):
        self._log.debug("sending message: {0}".format(message.control))

        # don't send a zero size body 
        if type(message.body) not in [list, tuple, type(None), ]:
            if len(message.body) == 0:
                message = message._replace(body=None)
            else:
                message = message._replace(body=[message.body, ])

        if message.body is None:
            self._rep_socket.send_json(message.control)
        else:
            self._rep_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._rep_socket.send(segment, zmq.SNDMORE)
            self._rep_socket.send(message.body[-1])

    def _receive_message(self):
        try:
            control = self._rep_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        body = []
        while self._rep_socket.rcvmore:
            body.append(self._rep_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return message_format(ident=None, control=control, body=body)

