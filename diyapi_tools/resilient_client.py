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
_ack_timeout = 10.0
_handshake_retry_interval = 60.0

_status_handshaking = 1
_status_connected = 2
_status_disconnected = 3

class ResilientClient(object):
    """
    a class that manages a zeromq XREQ socket as a client
    """
    polling_interval = 3.0

    def __init__(self, context, server_address, client_tag, client_address):
        self._log = logging.getLogger("ResilientClient-%s" % (
            server_address, 
        ))

        self._xreq_socket = context.socket(zmq.XREQ)
        self._xreq_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting")
        self._xreq_socket.connect(server_address)

        self._send_queue = deque()

        self._client_tag = client_tag
        self._client_address = client_address

        self._pending_message = None
        self._pending_message_start_time = None

        self._status = _status_disconnected
        self._status_time = time.time()

        self._send_handshake()

    def register(self, pollster):
        pollster.register_read(
            self._xreq_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._xreq_socket)

    def close(self):
        self._xreq_socket.close()

    @property
    def connected(self):
        return self._status is _status_connected

    def queue_message_for_send(self, message_control, data=None):
        """
        queue a message for send (unless we can send it immediately)
        if the message does not contain a message-id, we will supply one.
        """

        if not "message-id" in message_control:
            message_control["message-id"] = uuid.uuid1().hex

        message = _message_format(control=message_control, body=data)
        if self._status is _status_connected and self._pending_message is None:
            self._send_message(message)
        else:
            self._send_queue.append(message)

    def _send_handshake(self):
        self._log.info("sending handshake")
        message = {
            "message-type"      : "resilient-server-handshake",
            "message-id"        : uuid.uuid1().hex,
            "client-tag"        : self._client_tag,
            "client-address"    : self._client_address,
        }
        self._send_message(_message_format(control=message, body=None))
        self._status = _status_handshaking
        self._status_time = time.time()

    def _pollster_callback(self, _active_socket, readable, writable):
        message = self._receive_message()     

        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return None

        if self._pending_message is None:
            self._log.error("Unexpected message: %s" % (message.control, ))
            return

        expected_message_id = self._pending_message.control["message-id"]
        if message["message-id"] != expected_message_id:
            self._log.error("unknown ack %s expecting %s" %(
                message.control, self._pending_message.control 
            ))
            return

        # if we got and ack to a handshake request, we are connected
        if self._pending_message.control["message-type"] == \
            "resilient-server-handshake":
            assert self._status == _status_handshaking, self._status
            self._status = _status_connected
            self._status_time = time.time()

        self._pending_message = None
        self._pending_message_start_time = None

        try:
            message_to_send = self._send_queue.popleft()
        except IndexError:
            return

        self._send_message(message_to_send)
            
    def _send_message(self, message):
        self._log.info("sending message: %s" % (message.control, ))
        message.control["client-tag"] = self._client_tag

        # don't send a zero size body 
        if type(message.body) not in [list, tuple, type(None), ]:
            if len(message.body) == 0:
                message = message._replace(body=None)
            else:
                message = message._replace(body=[message.body, ])

        if message.body is None:
            self._xreq_socket.send_json(message.control)
        else:
            self._xreq_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._xreq_socket.send(segment, zmq.SNDMORE)
            self._xreq_socket.send(message.body[-1])

        self._pending_message = message
        self._pending_message_start_time = time.time()

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

        assert not self._xreq_socket.rcvmore()

    def run(self, halt_event):
        """
        time_queue task to check for timeouts and retries
        """
        if halt_event.is_set():
            self._log.info("halt event is set")
            return []

        if self._status == _status_connected:
            if  self._pending_message is not None:
                elapsed_time = time.time() - self._pending_message_start_time
                if elapsed_time > _ack_timeout:
                    self._log.warn(
                        "timeout waiting ack: treating as disconnect %s" % (
                            self._pending_message,
                        )
                    )
                    self._status = _status_disconnected
                    self._status_time = time.time()
                    # put the message at the head of the send queue 
                    self._send_queue.appendleft(self._pending_message)
                    self._pending_message = None
                    self._pending_message_start_time = None
        elif self._status == _status_disconnected:
            elapsed_time = time.time() - self._status_time 
            if elapsed_time > _handshake_retry_interval:
                self._send_handshake()
        elif self._status == _status_handshaking:
            assert self._pending_message is not None
            elapsed_time = time.time() - self._pending_message_start_time
            if elapsed_time > _ack_timeout:
                self._log.warn("timeout waiting handshake ack")
                self._status = _status_disconnected
                self._status_time = time.time()
                self._pending_message = None
                self._pending_message_start_time = None
        else:
            self._log.error("unknown status '%s'" % (self._status, ))

        return [(self.run, time.time() + self.polling_interval, ), ]

