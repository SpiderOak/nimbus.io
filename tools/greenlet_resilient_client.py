# -*- coding: utf-8 -*-
"""
resilient_client.py

a class that manages a zeromq DEALER (aka XREQ) socket as a client,
to a resilient server
"""
from collections import deque, namedtuple
import logging
import os
import random
import time
import uuid

import zmq

import gevent
from gevent.coros import RLock
from gevent.greenlet import Greenlet
from gevent_zeromq import zmq

# our internal message format
_message_format = namedtuple("Message", "control body")
_polling_interval = 3.0
_ack_timeout = float(os.environ.get("NIMBUSIO_ACK_TIMEOOUT", "10.0"))
_handshake_retry_interval = 60.0
_max_idle_time = 10 * 60.0
_reporting_interval = 60.0

_status_handshaking = 1
_status_connected = 2
_status_disconnected = 3

_status_name = {
    _status_handshaking     : "handshake",
    _status_connected       : "connected",
    _status_disconnected    : "disconnected",
}

class _GreenletResilientClientWatcher(Greenlet):
    """
    A class to watch a resilient client and handle various timeouts
    """
    def __init__(self, client):
        Greenlet.__init__(self)
        self._client = client
        self._log = logging.getLogger(str(self))
        self._prev_report_time = time.time()

    def _run(self):

        # start after a random interval so all watcher's aren't waking up at the
        # same time.
        gevent.sleep(_polling_interval * random.random())

        while True:
            self._client.test_current_status()

            elapsed_time = time.time() - self._prev_report_time
            if elapsed_time >= _reporting_interval:
                self._client.report_current_status()
                self._prev_report_time = time.time()

            gevent.sleep(_polling_interval)

    def __str__(self):
        return "watcher-%s" % (self._client._server_address, )

class GreenletResilientClient(object):
    """
    a class that manages a zeromq DEALER (aka XREQ) socket as a client
    """
    def __init__(
        self, 
        context, 
        pollster,
        server_node_name,
        server_address, 
        client_tag, 
        client_address,
        deliverator
    ):
        self._log = logging.getLogger("ResilientClient-%s" % (
            server_address, 
        ))

        self._context = context
        self._pollster = pollster
        self._server_node_name = server_node_name
        self._server_address = server_address

        self._dealer_socket = None

        self._client_tag = client_tag
        self._client_address = client_address
        self._deliverator = deliverator

        self._send_queue = deque()
        self._pending_message = None
        self._pending_message_start_time = None
        self._lock = RLock()

        # set the status time low so we fire off a handshake
        self._status = _status_disconnected
        self._log.info("status = %s" % (_status_name[self._status], ))
        self._status_time = 0.0

        self._last_successful_ack_time = 0.0

        self._dispatch_table = {
            _status_disconnected    : self._handle_status_disconnected,
            _status_connected       : self._handle_status_connected,            
            _status_handshaking     : self._handle_status_handshaking,  
        }

        self._watcher = _GreenletResilientClientWatcher(self)

        self._watcher.start()

    @property
    def connected(self):
        return self._status == _status_connected

    @property
    def server_node_name(self):
        return self._server_node_name

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

    def report_current_status(self):
        self._lock.acquire()
        try:
            if self._status == _status_connected:
                return
            elapsed_time = time.time() - self._status_time
            self._log.info("%s %d seconds; send queue size = %s" % (
                _status_name[self._status], 
                elapsed_time, 
                len(self._send_queue)
            ))
        finally:
            self._lock.release()

    def _handle_status_disconnected(self):
        elapsed_time = time.time() - self._status_time 
        if elapsed_time < _handshake_retry_interval:
            return

        assert self._dealer_socket is None
        self._dealer_socket = self._context.socket(zmq.XREQ)
        self._dealer_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting to server")
        self._dealer_socket.connect(self._server_address)
        self._pollster.register_read(
            self._dealer_socket, self._pollster_callback
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
        self._log.info("status = %s" % (_status_name[self._status], ))
        self._status_time = time.time()

        self._send_message(message)

    def _handle_status_connected(self):

        if self._pending_message_start_time is None:
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

        # deliver a failure reply to whoever is waiting for this message
        reply = {
            "message-type"  : "ack-timeout-reply",
            "message-id"    : self._pending_message.control["message-id"],
            "result"        : "ack timeout",
            "error-message" : "timeout waiting ack: treating as disconnect",
        }

        message = _message_format(control=reply, body=None)
        self._deliverator.deliver_reply(message)

        # heave the message; we're heaving the whole request
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
        self._log.debug("disconnecting")
        assert self._dealer_socket is not None
        self._pollster.unregister(self._dealer_socket)
        self._dealer_socket.close()
        self._dealer_socket = None

        self._status = _status_disconnected
        self._log.info("status = %s" % (_status_name[self._status], ))
        self._status_time = time.time()

    def close(self):
        self._watcher.kill()
        self._watcher.join()

        if self._dealer_socket is not None:
            self._disconnect()

    def queue_message_for_send(self, message_control, data=None):

        if not "message-id" in message_control:
            message_control["message-id"] = uuid.uuid1().hex

        message_id = message_control["message-id"]
        delivery_channel = self._deliverator.add_request(message_id)

        message = _message_format(control=message_control, body=data)

        # if we don't queue the message, that means we can send it right now
        self._lock.acquire()
        try:
            if self._status is _status_connected \
            and self._pending_message is None:
                self._pending_message = message
                self._pending_message_start_time = time.time()
                self._send_message(message)
            else:
                self._send_queue.append(message)

        finally:
            self._lock.release()

        return delivery_channel

    def _pollster_callback(self, _active_socket, readable, writable):
        self._lock.acquire()
        try:
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
                    message, self._pending_message 
                ))
                return

            message_type = self._pending_message.control["message-type"]
            self._log.debug("received ack: %s %s" % (
                message_type, message["message-id"],
            ))
            self._last_successful_ack_time = time.time()

            # if we got an ack to a handshake request, we are connected
            if message_type == "resilient-server-handshake":
                assert self._status == _status_handshaking, self._status
                self._status = _status_connected
                self._log.info("status = %s" % (_status_name[self._status], ))
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
        finally:
            self._lock.release()

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
            self._dealer_socket.send_json(message.control)
        else:
            self._dealer_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._dealer_socket.send(segment, zmq.SNDMORE)
            self._dealer_socket.send(message.body[-1])

    def _receive_message(self):
        # we should only be receiving ack, so we don't
        # check for multipart messages
        try:
            return self._dealer_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        assert not self._dealer_socket.rcvmore()

    def __str__(self):
        return "ResilientClient-%s" % (self._server_node_name, )

