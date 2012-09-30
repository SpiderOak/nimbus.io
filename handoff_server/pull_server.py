# -*- coding: utf-8 -*-
"""
pull_server.py

a class that manages a zeromq PULL socket as a server,
to multiple PUSH clients
"""
import logging

from  gevent.greenlet import Greenlet
from gevent_zeromq import zmq

from tools.zeromq_util import prepare_ipc_path

class PULLServer(Greenlet):
    """
    context
        zeromq context

    address
        the address our socket binds to. Sent to the remote server by the 
        resilient client.

    reply_queue
        queue for incoming messages, which are replies to messages
        we have sent to resiliant servers

    halt_event:
        Event object, set when it's time to halt

    Each process maintains a single PULL_ socket for all of its resilient 
    clients. This class wraps that socket.

    .. _PULL: http://api.zeromq.org/2-1:zmq-socket#toc13
    """
    def __init__(self, context, address, reply_queue, halt_event):
        Greenlet.__init__(self)
        self._name = "PULLServer"

        self._log = logging.getLogger(self._name)

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._pull_socket = context.socket(zmq.PULL)
        self._log.debug("bindingi to {0}".format(address))
        self._pull_socket.bind(address)

        self._reply_queue = reply_queue
        self._halt_event = halt_event

    def __str__(self):
        return self._name

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        self._pull_socket.close()
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        while not self._halt_event.is_set():
            try:
                control = self._pull_socket.recv_json(zmq.NOBLOCK)
            except zmq.ZMQError, instance:
                if instance.errno == zmq.EAGAIN:
                    self._halt_event.wait(1.0)
                    continue
                raise
            body = []
            while self._pull_socket.rcvmore:
                body.append(self._pull_socket.recv())

            # 2011-04-06 dougfort -- if someone is expecting a list and we 
            # only get one segment, they are going to have to deal with it.
            if len(body) == 0:
                body = None
            elif len(body) == 1:
                body = body[0]

            self._log.debug("received: %s" % (control, ))
            self._reply_queue.put((control, body, ))

