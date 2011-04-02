# -*- coding: utf-8 -*-
"""
greenlet_xreq_client.py

a class that manages a zeromq XREQ socket as a client,
to an XREP server
"""
from collections import namedtuple
import logging
import uuid

import gevent
from gevent.queue import Queue
from gevent_zeromq import zmq

# our internal message format
_message_format = namedtuple("Message", "control body")

class GreenletXREQClient(object):
    """
    a class that manages a zeromq XREQ socket as a client
    """
    def __init__(self, context, node_name, address):
        self._log = logging.getLogger("XREQClient-%s" % (node_name, ))

        self._xreq_socket = context.socket(zmq.XREQ)
        self._log.debug("connecting to %s" % (address, ))
        self._xreq_socket.connect(address)

        self._send_queue = Queue(maxsize=None)
        self._delivery_queues = dict()

    def register(self, pollster):
        pollster.register_read_or_write(
            self._xreq_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._xreq_socket)

    def close(self):
        self._xreq_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        """
        message control must be a dict 
        If the caller includes a 'request-id' key we will use it,
        otherwise, we will supply one.
        return: a gevent queue (zero size Queue) the the reply will
        be deliverd.
        """
        if not "request-id" in message_control:
            message_control["request-id"] = uuid.uuid1().hex
        self._delivery_queues[message_control["request-id"]] = \
            Queue(maxsize=None)
        self._send_queue.put(
            _message_format(control=message_control, body=data)
        )
        return self._delivery_queues[message_control["request-id"]]

    def _pollster_callback(self, _active_socket, readable, writable):
        # push our output first
        if writable:
            while not self._send_queue.empty():
                message = self._send_queue.get_nowait()
                self._send_message(message)

        # if we have input, read it and queue it
        if readable:
            message = self._receive_message()      
            # if we get None, that means the socket would have blocked
            # go back and wait for more
            if message is None:
                return None
            if not "request-id" in message.control:
                self._log.error("message has no 'request-id' %s" % (
                    message.control
                ))
            else:
                try:
                    delivery_queue = self._delivery_queues.pop(
                        message.control["request-id"]
                    )
                except KeyError:
                    self._log.error("No delivery queue for %s" % (
                        message.control, 
                    ))
                else:
                    delivery_queue.put((message.control, message.body, ))

    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))
        if message.body is not None:
            self._xreq_socket.send_json(message.control, zmq.SNDMORE)
            self._xreq_socket.send(message.body, copy=False)
        else:
            self._xreq_socket.send_json(message.control)

    def _receive_message(self):
        try:
            control = self._xreq_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        if self._xreq_socket.rcvmore():
            body = self._xreq_socket.recv()
        else:
            body = None

        return _message_format(control=control, body=body)

