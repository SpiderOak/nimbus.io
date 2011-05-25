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
_max_idle_time = 10 * 60.0

_status_handshaking = 1
_status_connected = 2
_status_disconnected = 3

_status_name = {
    _status_handshaking     : "handshake",
    _status_connected       : "connected",
    _status_disconnected    : "disconnected",
}

class ResilientClient(object):
    """
    a class that manages a zeromq XREQ socket as a client
    """
    polling_interval = 3.0

    def __init__(
        self, 
        context, 
        pollster,
        server_node_name,
        server_address, 
        client_tag, 
        client_address
    ):
        self._log = logging.getLogger("ResilientClient-%s" % (
            server_address, 
        ))

        self._context = context
        self._pollster = pollster
        self._server_node_name = server_node_name
        self._server_address = server_address

        self._xreq_socket = None

        self._send_queue = deque()

        self._client_tag = client_tag
        self._client_address = client_address

        self._pending_message = None
        self._pending_message_start_time = None

        self._status = _status_disconnected
        self._status_time = 0.0

        self._last_successful_ack_time = 0.0

        self._dispatch_table = {
            _status_disconnected    : self._handle_status_disconnected,
            _status_connected       : self._handle_status_connected,            
            _status_handshaking     : self._handle_status_handshaking,  
        }

    @property
    def connected(self):
        return self._status == _status_connected

    @property
    def server_node_name(self):
        return self._server_node_name

    def _handle_status_disconnected(self):
        elapsed_time = time.time() - self._status_time 
        if elapsed_time < _handshake_retry_interval:
            return

        assert self._xreq_socket is None
        self._xreq_socket = self._context.socket(zmq.XREQ)
        self._xreq_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting to server")
        self._xreq_socket.connect(self._server_address)
        self._pollster.register_read(
            self._xreq_socket, self._pollster_callback
        )

        message = {
            "message-type"      : "resilient-server-handshake",
            "message-id"        : uuid.uuid1().hex,
            "client-tag"        : self._client_tag,
            "client-address"    : self._client_address,
        }
        message = _message_format(control=message, body=None)
        self._pending_message = message
        self._pending_message_start_time = time.time()
        self._status = _status_handshaking
        self._status_time = time.time()

        self._send_message(message)

    def _handle_status_connected(self):

        # if we think we are connected, but we haven't sent anything
        # recently, disconnect until we have something to send
        if  self._pending_message is None:
            elapsed_time = time.time() - self._last_successful_ack_time
            if elapsed_time >= _max_idle_time:
                self._log.info("idle for %s seconds, disconnecting" % (
                    elapsed_time,
            ))
            self._disconnect()
            return

        elapsed_time = time.time() - self._pending_message_start_time
        if elapsed_time < _ack_timeout:
            return

        self._log.error(
            "timeout waiting ack: treating as disconnect %s" % (
                self._pending_message.control,
            )
        )
        self._disconnect()

        # put the message at the head of the send queue 
        self._send_queue.appendleft(self._pending_message)

        self._pending_message = None
        self._pending_message_start_time = None

    def _handle_status_handshaking(self):    
        assert self._pending_message is not None
        elapsed_time = time.time() - self._pending_message_start_time
        if elapsed_time < _ack_timeout:
            return

        self._log.warn("timeout waiting handshake ack")

        self._disconnect()

        self._pending_message = None
        self._pending_message_start_time = None

    def _disconnect(self):
        assert self._xreq_socket is not None
        self._pollster.unregister(self._xreq_socket)
        self._xreq_socket.close()
        self._xreq_socket = None

        self._status = _status_disconnected
        self._status_time = time.time()

    def close(self):
        if self._xreq_socket is not None:
            self._disconnect()

    def queue_message_for_send(self, message_control, data=None):
        """
        queue a message for send (unless we can send it immediately)
        if the message does not contain a message-id, we will supply one.
        """
        if not "message-id" in message_control:
            message_control["message-id"] = uuid.uuid1().hex

        message = _message_format(control=message_control, body=data)

        if self._status is _status_connected and self._pending_message is None:
            self._pending_message = message
            self._pending_message_start_time = time.time()
            self._send_message(message)
        else:
            self._send_queue.append(message)

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

        # successful ack
        message_type = self._pending_message.control["message-type"]
        self._log.debug("received ack: %s %s" % (
            message_type, message["message-id"],
        ))
        self._last_successful_ack_time = time.time()

        # if we got and ack to a handshake request, we are connected
        if message_type == "resilient-server-handshake":
            assert self._status == _status_handshaking, self._status
            self._status = _status_connected
            self._status_time = time.time()

        self._pending_message = None
        self._pending_message_start_time = None

        try:
            message_to_send = self._send_queue.popleft()
        except IndexError:
            return

        self._pending_message = message_to_send
        self._pending_message_start_time = time.time()

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

        self._dispatch_table[self._status]()

        return [(self.run, time.time() + self.polling_interval, ), ]

    def __str__(self):
        return "ResilientClient-%s" % (self._server_address, )

