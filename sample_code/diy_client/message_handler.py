# -*- coding: utf-8 -*-
"""
message_handler.py

class MessageHandler

A greenlet object to manage messages from the receive_queue
"""
import logging

import gevent
from gevent_zeromq import zmq
from  gevent.greenlet import Greenlet

class MessageHandler(Greenlet):
    """
    A greenlet object to manage messages from the receive_queue
    """
    def __init__(self, halt_event, context, pub_address, receive_queue):
        Greenlet.__init__(self)
        self._log = logging.getLogger("MessageHandler")
        self._halt_event = halt_event
        self._pub_socket = context.socket(zmq.PUB)
        self._pub_socket.setsockopt(zmq.LINGER, 1000)
        self._pub_address = pub_address
        self._receive_queue = receive_queue

    def join(self, timeout=None):
        """
        close the _pub socket
        """
        if self._pub_socket is not None:
            self._pub_socket.close()
            self._pub_socket = None
        Greenlet.join(self, timeout)

    def _run(self):
        self._log.info("binding PUB socket to %s" % (self._pub_address, ))
        self._pub_socket.bind(self._pub_address)

        while not self._halt_event.is_set():
            message, body = self._receive_queue.get()

            self._log.info(str(message))

            status_message = {
                "message-type"  : message["client-topic"],
                "completed"     : True,        
            }

            self._pub_socket.send(status_message["message-type"], zmq.SNDMORE)
            self._pub_socket.send_json(status_message)
        
