# -*- coding: utf-8 -*-
"""
push_client.py

a class that manages a zeromq PUSH socket as a client,
The purpose is to have multiple clients pushing to a single PULL server
"""
import logging

import zmq

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

    def close(self):
        self._push_socket.close()

    def send(self, message, data=None):
        self._log.info("sending message: %s" % (
            message,  
        ))
        if data is not None:
            self._push_socket.send_json(message, zmq.SNDMORE)
            self._push_socket.send(data, copy=False)
        else:
            self._push_socket.send_json(message)

