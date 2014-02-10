# -*- coding: utf-8 -*-
"""
push_client.py

a class that manages a zeromq PUSH socket as a client,
The purpose is to have multiple clients pushing to a single PULL server
"""
import logging

import zmq

from tools.zeromq_util import set_send_hwm

_push_hwm = 100

class PUSHClient(object):
    """
    a class that manages a zeromq PUSH socket as a client,
    The purpose is to have multiple clients pushing to a single PULL server
    """
    def __init__(self, context, address):
        self._log = logging.getLogger("PUSH.{0}".format(address))

        self._push_socket = context.socket(zmq.PUSH)
        set_send_hwm(self._push_socket, _push_hwm)
        self._push_socket.setsockopt(zmq.LINGER, 5000)
        self._log.debug("connecting")
        self._push_socket.connect(address)

    def close(self):
        self._push_socket.close()

    def send(self, message, data=None):
        self._log.debug("sending message: {0}".format(message))

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

