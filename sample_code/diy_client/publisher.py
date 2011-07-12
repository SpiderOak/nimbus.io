# -*- coding: utf-8 -*-
"""
publisher.py

A greenlet to manage the PUB socket
"""
import logging

import gevent
from gevent_zeromq import zmq
from  gevent.greenlet import Greenlet

class Publisher(Greenlet):
    """
    A greenlet object to manage a zeromq PUB socket
    """
    def __init__(self, halt_event, context, pub_address, send_queue):
        Greenlet.__init__(self)
        self._log = logging.getLogger("Publisher")
        self._halt_event = halt_event
        self._pub_socket = context.socket(zmq.PUB)
        self._pub_address = pub_address
        self._send_queue = send_queue

    def join(self, timeout=None):
        """
        close the _pull socket
        """
        Greenlet.join(self, timeout)
        if self._pub_socket is not None:
            self._pub_socket.close()
            self._pub_socket = None

    def _run(self):
        self._log.info("binding PUB socket to %s" % (self._pub_address, ))
        self._pub_socket.bind(self._pub_address)

        while not self._halt_event.is_set():
            message, body = self._send_queue.get()
            self._log.info(str(message))
        
            self._pub_socket.send(message["message-type"], zmq.SNDMORE)
            if body is not None and len(body) > 0:
                self._pub_socket.send_json(message, zmq.SNDMORE)
                for item in body[:-1]:
                    self._pub_socket.send(item, zmq.SNDMORE)
                self._pub_socket.send(body[-1])
            else:
                self._pub_socket.send_json(message)

