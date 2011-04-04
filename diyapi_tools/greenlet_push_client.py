# -*- coding: utf-8 -*-
"""
greenlet_push_client.py

a class that manages a zeromq PUSH socket as a client,
The purpose is to have multiple clients pushing to a single PULL server
"""
from collections import namedtuple
import logging

from gevent.queue import Queue
from gevent_zeromq import zmq

_message_format = namedtuple("Message", "control body")

class GreenletPUSHClient(object):
    """
    a class that manages a zeromq PUSH socket as a client,
    The purpose is to have multiple clients pushing to a single PULL server
    """
    def __init__(self, context, node_name, address):
        self._log = logging.getLogger("PUSHClient-%s" % (node_name, ))

        self._push_socket = context.socket(zmq.PUSH)
        self._log.debug("connecting to%s" % (address, ))
        self._push_socket.connect(address)

        self._send_queue = Queue(maxsize=None)

    def register(self, pollster):
        pollster.register_write(
            self._push_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._push_socket)

    def close(self):
        self._push_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        self._send_queue.put_nowait(
            _message_format(control=message_control, body=data)
        )

    def _pollster_callback(self, _active_socket, readable, writable):
        if writable:
            while not self._send_queue.empty():
                message = self._send_queue.get_nowait()
                self._send_message(message)

    def _send_message(self, message):
        self._log.info("sending message: %s" % (
            message.control,  
        ))
        if message.body is not None:
            self._push_socket.send_json(message.control, zmq.SNDMORE)
            self._push_socket.send(message.body, copy=False)
        else:
            self._push_socket.send_json(message.control)

