# -*- coding: utf-8 -*-
"""
pub_server.py

a class that manages a zeromq PUB socket as a server,
"""
import logging

import zmq

from tools.zeromq_util import prepare_ipc_path

class PUBServer(object):
    """
    a class that manages a zeromq PUB socket as a server
    """
    def __init__(self, context, address):
        self._log = logging.getLogger("PUBServer-%s" % (address, ))

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._pub_socket = context.socket(zmq.PUB)
        self._pub_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("binding")
        self._pub_socket.bind(address)

    def close(self):
        self._pub_socket.close()

    def send_message(self, message):
        self._log.debug("sending message: %s %s" % (
            message["message-type"], message,  
        ))
        self._pub_socket.send(str(message["message-type"]), zmq.SNDMORE)
        self._pub_socket.send_json(message)

