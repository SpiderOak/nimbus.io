# -*- coding: utf-8 -*-
"""
sub_client.py

a class that manages a zeromq SUB socket as a client,
to a PUB server
"""
import logging

import zmq

class SUBClient(object):
    """
    a class that manages a zeromq SUB socket as a client,
    """
    def __init__(self, context, address, topics, receive_queue):
        self._log = logging.getLogger("SUBClient-%s" % (address, ))

        self._sub_socket = context.socket(zmq.SUB)
        self._log.debug("connecting")
        self._sub_socket.connect(address)
        self._sub_socket.setsockopt(zmq.SUBSCRIBE, topics)

        self._receive_queue = receive_queue

    def register(self, pollster):
        pollster.register_read(self._sub_socket, self._pollster_callback)

    def unregister(self, pollster):
        pollster.unregister(self._sub_socket)

    def close(self):
        self._sub_socket.close()

    def _pollster_callback(self, _active_socket, readable, writable):
        message = self._receive_message()      
        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return None
        self._receive_queue.append((message, None, ))
            
    def _receive_message(self):
        try:
            topic = self._sub_socket.recv(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        assert self._sub_socket.rcvmore(), "expecting actual message"
        message = self._sub_socket.recv_json()
        assert message["message-type"] == topic, message

        return message

