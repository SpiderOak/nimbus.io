# -*- coding: utf-8 -*-
"""
pull_server.py

class PULLServer

A greenlet object to manage a zeromq PULL socket
"""
import logging

import gevent
from gevent_zeromq import zmq
from  gevent.greenlet import Greenlet

class PULLServer(Greenlet):
    """
    A greenlet object to manage a zeromq PULL socket
    """
    def __init__(self, halt_event, context, pull_address, receive_queue):
        Greenlet.__init__(self)
        self._log = logging.getLogger("PULLServer")
        self._halt_event = halt_event
        self._pull_socket = context.socket(zmq.PULL)
        self._pull_address = pull_address
        self._receive_queue = receive_queue

    def join(self, timeout=None):
        """
        close the _pull socket
        """
        Greenlet.join(self, timeout)
        if self._pull_socket is not None:
            self._pull_socket.close()
            self._pull_socket = None

    def _run(self):
        self._log.info("run")
        self._log.info("binding to %s" % (self._pull_address, ))
        self._pull_socket.bind(self._pull_address)
        
        while not self._halt_event.is_set():
            # 2011-07-12 dougfort -- this blocks the whole process
            try:
                message = self._pull_socket.recv_json(zmq.NOBLOCK)        
            except zmq.ZMQError, instance:
                if instance.errno == zmq.EAGAIN:
                    gevent.sleep(1.0)
                    continue
                raise
            body = list()
            while self._pull_socket.rcvmore():
                body.append(self._pull_socket.recv())

            self._receive_queue.put((message, body, ))
        
