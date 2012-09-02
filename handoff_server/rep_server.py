# -*- coding: utf-8 -*-
"""
rep_server.py

a class that manages a zeromq REP socket as a server,
"""
import logging

from  gevent.greenlet import Greenlet
from gevent_zeromq import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

class REPServer(Greenlet):
    """
    zmq_context
        zeromq context

    address
        the address our socket binds to. 

    request_queue
        queue for incoming request messages

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, zmq_context, address, request_queue, halt_event):
        Greenlet.__init__(self)

        self._log = logging.getLogger("REPServer")

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._rep_socket = zmq_context.socket(zmq.REP)
        self._log.debug("binding to {0}".format(address))
        self._rep_socket.bind(address)

        self._request_queue = request_queue

        self._halt_event = halt_event

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        self._rep_socket.close()
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        ping_count = 0
        while not self._halt_event.is_set():
            try:
                control = self._rep_socket.recv_json(zmq.NOBLOCK)
            except zmq.ZMQError, instance:
                if instance.errno == zmq.EAGAIN:
                    self._halt_event.wait(1.0)
                    continue
                raise
            body = []
            while self._rep_socket.rcvmore:
                body.append(self._rep_socket.recv())

            if len(body) == 0:
                body = None
            elif len(body) == 1:
                body = body[0]

            message = message_format(ident=None, control=control, body=body)

            # ack is sufficient rely to ping
            if message.control["message-type"] == "ping":
                ping_count += 1
                if ping_count % 100 == 0:
                    self._log.debug("{0} pings".format(ping_count))
            else:
                self._log.debug("received: %s" % (message.control, ))
                self._request_queue.put(message)

            ack_message = {
                "message-type" : "resilient-server-ack",
                "message-id"   : control["message-id"],
                "incoming-type": control["message-type"],
                "accepted"     : True
            }

            self._rep_socket.send_json(ack_message)



