# -*- coding: utf-8 -*-
"""
greenlet_pull_server.py

a class that manages a zeromq PULL socket as a server,
to multiple PUSH clients
"""
import logging

from  gevent.greenlet import Greenlet
import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

class GreenletPULLServer(Greenlet):
    """
    context
        zeromq context

    address
        the address our socket binds to. Sent to the remote server by the 
        resilient client.

    deliverator
        we pass incoming messages to the Deliverator, which delivers them
        to greenlets that are waiting for them.

    Each process maintains a single PULL_ socket for all of its resilient 
    clients. This class wraps that socket.

    .. _PULL: http://api.zeromq.org/2-1:zmq-socket#toc13
    """
    def __init__(self, context, address, deliverator):
        Greenlet.__init__(self)

        self._log = logging.getLogger("PULLServer-%s" % (address, ))

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._pull_socket = context.socket(zmq.PULL)
        self._log.debug("binding")
        self._pull_socket.bind(address)

        self._deliverator = deliverator

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        self._pull_socket.close()
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        while True:
            control = self._pull_socket.recv_json()

            body = []
            while self._pull_socket.rcvmore:
                body.append(self._pull_socket.recv())

            # 2011-04-06 dougfort -- if someone is expecting a list and we 
            # only get one segment, they are going to have to deal with it.
            if len(body) == 0:
                body = None
            elif len(body) == 1:
                body = body[0]

            message = message_format(ident=None, control=control, body=body)
            self._log.debug("received: %s" % (message.control, ))
            self._deliverator.deliver_reply(message)

