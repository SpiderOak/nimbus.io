# -*- coding: utf-8 -*-
"""
resilient_client.py

a class that manages a zeromq XREQ socket as a client,
to a resilient server
"""
from collections import deque, namedtuple
import logging
import time
import uuid

import zmq

# our internal message format
_message_format = namedtuple("Message", "control body")

class ResilientClient(object):
    """
    a class that manages a zeromq XREQ socket as a client
    """
    def __init__(self, context, server_address, client_tag, client_address):
        self._log = logging.getLogger("ResilientClient-%s" % (
            server_address, 
        ))

        self._xreq_socket = context.socket(zmq.XREQ)
        self._log.debug("connecting")
        self._xreq_socket.connect(server_address)

        self._send_queue = deque()

        self._client_tag = client_tag
        self._client_address = client_address

        self._pending_ack = None
        self._pending_ack_start_time = None

    def register(self, pollster):
        pollster.register_read(
            self._xreq_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._xreq_socket)

    def close(self):
        self._xreq_socket.close()

    def queue_message_for_send(self, message_control, data=None):
        if self._pending_ack is None:
            self._send_message(
                _message_format(control=message_control, body=data)
            )
        else:
            self._send_queue.append(
                _message_format(control=message_control, body=data)
            )

    def _send_handshake(self):
        assert self._pending_ack is None
        message = {
            "message-type"      : "resilient-server-handshake",
            "request-id"        : uuid.uuid1().hex,
            "client-tag"        : self._client_tag,
            "client-address"    : self._client_address,
        }
        self._send_message(_message_format(control=message, body=None))

    def _pollster_callback(self, _active_socket, readable, writable):
        message = self._receive_message()     

        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return None

        assert self._pending_ack is not None
        if message["request-id"] != self._pending_ack:
            self._log.error("unknown ack %s" %(message, ))
            return

        self._pending_ack = None
        self._pending_ack_start_time = None

        try:
            message_to_send = self._send_queue.popleft()
        except IndexError:
            return

        self._send_message(message_to_send)
            
    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))
        message.control["client-tag"] = self._client_tag
        if message.body is not None:
            self._xreq_socket.send_json(message.control, zmq.SNDMORE)
            if type(message.body) not in [list, tuple, ]:
                message = message._replace(body=[message.body, ])
            for segment in message.body[:-1]:
                self._xreq_socket.send(segment, zmq.SNDMORE)
            self._xreq_socket.send(message.body[-1])
        else:
            self._xreq_socket.send_json(message.control)

        self._pending_ack = message.control["request-id"]
        self._pending_ack_start_time = time.time()

    def _receive_message(self):
        # we should only be receiving ack, so we don't
        # check for multipart messages
        try:
            return self._xreq_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

