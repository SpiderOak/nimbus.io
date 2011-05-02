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

import gevent
from gevent.coros import RLock
from gevent.greenlet import Greenlet
from gevent_zeromq import zmq

# our internal message format
_message_format = namedtuple("Message", "control body")
_polling_interval = 3.0
_ack_timeout = 10.0
_handshake_retry_interval = 60.0

_status_handshaking = 1
_status_connected = 2
_status_disconnected = 3

class _GreenletResilientClientState(object):
    """
    a class that controls access to the mutable state of a resilient client.
    """
    def __init__(self, client_tag, client_address):
        self._log = logging.getLogger("resilient-client-state-%s" % (
            client_tag, 
        ))
        self._client_tag = client_tag
        self._client_address = client_address
        # note that we treat pending_message as en extension of the queue
        # it should be locked whenever the queue is locked
        self._send_queue = deque()
        self._pending_message = None
        self._pending_message_start_time = None
        self._lock = RLock()
        self._status = _status_disconnected
        # set the status time low so we fire off a handshake
        self._status_time = 0.0

        self._dispatch_table = {
            _status_disconnected    : self._handle_status_disconnected,
            _status_connected       : self._handle_status_connected,            
            _status_handshaking     : self._handle_status_handshaking,  
        }

    def test_current_status(self):
        """
        check for timeouts based on current state
        may return a handshake message to send
        """
        self._lock.acquire()
        try:
            return self._dispatch_table[self._status]()
        finally:
            self._lock.release()

    def queue_message_for_send(self, message):
        """
        If we are in a state such that the message can be sent immediately,
            don't queue it
            return False
        otherwise
            add the message to the queue
            return True
        """
        self._lock.acquire()
        try:
            if self._status is _status_connected \
            and self._pending_message is None:
                self._pending_message = message
                self._pending_message_start_time = time.time()
                return False
        
            self._send_queue.append(message)
            return True

        finally:
            self._lock.release()

    def test_incoming_message(self, message):
        """
        if this is an ack to an internal (handshake) message
            set status to connected

        if this message is the ack we are expecting
        and we can pop a message from the send queue
            return that message
        """
        self._lock.acquire()
        try:
            if self._pending_message is None:
                self._log.error("Unexpected message: %s" % (message.control, ))
                return

            expected_request_id = self._pending_message.control["request-id"]
            if message["request-id"] != expected_request_id:
                self._log.error("unknown ack %s expecting %s" %(
                    message, self._pending_message 
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

            self._pending_message = message_to_send
            self._pending_message_start_time = time.time()

            return message_to_send
        finally:
            self._lock.release()

    def _handle_status_disconnected(self):
        elapsed_time = time.time() - self._status_time 
        if elapsed_time < _handshake_retry_interval:
            return

        message = {
            "message-type"      : "resilient-server-handshake",
            "request-id"        : uuid.uuid1().hex,
            "client-tag"        : self._client_tag,
            "client-address"    : self._client_address,
        }
        message = _message_format(control=message, body=None)
        self._pending_message = message
        self._pending_message_start_time = time.time()
        self._status = _status_handshaking
        self._status_time = time.time()

        return message

    def _handle_status_connected(self):
        if  self._pending_message is None:
            return

        elapsed_time = time.time() - self._pending_message_start_time
        if elapsed_time < _ack_timeout:
            return

        self._log.warn(
            "timeout waiting ack: treating as disconnect %s" % (
                self._pending_message.control,
            )
        )
        self._status = _status_disconnected
        self._status_time = time.time()
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
        self._status = _status_disconnected
        self._status_time = time.time()
        self._pending_message = None
        self._pending_message_start_time = None

class _GreenletResilientClentStateWatcher(Greenlet):
    """
    A class to watch the mustable state of a resilient client and handle 
    various timeouts
    """
    def __init__(self, client_tag, state, send_function):
        Greenlet.__init__(self)
        self._client_tag = client_tag
        self._state = state
        self._send_function = send_function
        self._log = logging.getLogger(str(self))

    def _run(self):
        while True:
            message_to_send = self._state.test_current_status()
            if message_to_send is not None:
                self._send_function(message_to_send)
            gevent.sleep(_polling_interval)

    def __str__(self):
        return "state-watcher-%s" % (self._client_tag, )

class GreenletResilientClient(object):
    """
    a class that manages a zeromq XREQ socket as a client
    """
    def __init__(
        self, 
        context, 
        server_address, 
        client_tag, 
        client_address,
        deliverator
    ):
        self._log = logging.getLogger("ResilientClient-%s" % (
            server_address, 
        ))

        self._xreq_socket = context.socket(zmq.XREQ)
        self._log.debug("connecting")
        self._xreq_socket.connect(server_address)

        self._client_tag = client_tag
        self._client_address = client_address
        self._deliverator = deliverator

        self._state = _GreenletResilientClientState(client_tag, client_address)
        self._state_watcher = _GreenletResilientClentStateWatcher(
            client_tag, self._state, self._send_message
        )
        self._state_watcher.start()

    def register(self, pollster):
        pollster.register_read(
            self._xreq_socket, self._pollster_callback
        )

    def unregister(self, pollster):
        pollster.unregister(self._xreq_socket)

    def close(self):
        self._state_watcher.kill()
        self._state_watcher.join()
        self._xreq_socket.close()

    def queue_message_for_send(self, message_control, data=None):

        if not "request-id" in message_control:
            message_control["request-id"] = uuid.uuid1().hex

        request_id = message_control["request-id"]
        delivery_channel = self._deliverator.add_request(request_id)

        message = _message_format(control=message_control, body=data)

        # if we don't queue the message, that means we can send it right now
        if not self._state.queue_message_for_send(message):
            self._send_message(message)

        return delivery_channel

    def _pollster_callback(self, _active_socket, readable, writable):
        message = self._receive_message()     

        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return None

        message_to_send = self._state.test_incoming_message(message)

        if message_to_send is not None:
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

