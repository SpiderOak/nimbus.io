# -*- coding: utf-8 -*-
"""
greenlet_push_client.py

a class that manages a zeromq PUSH socket as a client,
The purpose is to have multiple clients pushing to a single PULL server
"""
import logging

import zmq

class GreenletPUSHClient(object):
    """
    a class that manages a zeromq PUSH socket as a client,
    The purpose is to have multiple clients pushing to a single PULL server
    """
    def __init__(self, context, node_name, address):
        self._log = logging.getLogger("PUSHClient-%s" % (node_name, ))

        self._push_socket = context.socket(zmq.PUSH)
        self._push_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting to%s" % (address, ))
        self._push_socket.connect(address)

    def close(self):
        self._push_socket.close()

    def send(self, message, data=None):
        self._log.debug("sending message: %s" % (message))

        # don't send a zero size body 
        if type(data) not in [list, tuple, type(None), ]:
            if len(data) == 0:
                data = None
            else:
                data = [data, ]

        if data is None:
            self._push_socket.send_json(message)
        else:
            self._push_socket.send_json(message, zmq.SNDMORE)
            for segment in data[:-1]:
                self._push_socket.send(segment, zmq.SNDMORE)
            self._push_socket.send(data[-1])

