# -*- coding: utf-8 -*-
"""
resilient_client.py

a class that manages a zeromq DEALER (aka XREQ) socket as a client,
to a resilient server
"""
import logging
import os
import time
import uuid

from  gevent.greenlet import Greenlet
import gevent.queue
from gevent.coros import RLock
from gevent_zeromq import zmq

from tools.data_definitions import message_format

_polling_interval = 3.0
_ack_timeout = float(os.environ.get("NIMBUSIO_ACK_TIMEOOUT", "10.0"))
_handshake_retry_interval = 60.0
_max_idle_time = 10 * 60.0
_reporting_interval = 60.0
_connect_delay = 60.0

_status_handshaking = 1
_status_connected = 2
_status_disconnected = 3

_status_name = {
    _status_handshaking     : "handshake",
    _status_connected       : "connected",
    _status_disconnected    : "disconnected",
}

class GreenletResilientClient(Greenlet):
    """
    context
        zeromq context

    server_node_name
        The node name of the server we connect to
        
    server_address
        The zeromq address of the ROUTER_ socket of the server we connect to

    client_tag
        A unique identifier for our client, to be inclused in every message
        so the remote server knows where to send replies

    client_address
        the address our socket binds to. Sent to the remote server in the 
        initial handshake

    deliverator
        handle to the deliverator object

    GreenletResilientClient uses two zeromq patterns to maintain a connection
    to a resilient server.

    - request-reply_ for sending messages and for receiving acknowledgements
    - pipeline_ for receiving the actual replies to messages

    Each process maintains a single PULL_ socket for all of its resilient 
    clients. The address of this socket is the member **_client_address**.
    The client includes this in every message along with **_client_tag** which
    uniquely identifies the client to the server

    Each resilient client maintains its own DEALER_ socket **_dealer_socket**.

    At startup the client sends a *handshake* message to the server. The client
    is not considered connected until it gets an ack fro the handshake.

    Normal workflow:
    
    1. The client pops a message from **_send_queue**
    2. The client sends the message over the DEALER_ socket
    3. The client waits for and ack from the server. It does not send any
       other messages until it gets the ack.
    4. When the ack is received the client repeats from step 1.
    5. The actual reply from the server comes to the PULL_ socket and is
       handled outside the client

    .. _ROUTER: http://api.zeromq.org/2-1:zmq-socket#toc7
    .. _request-reply: http://www.zeromq.org/sandbox:dealer
    .. _pipeline: http://api.zeromq.org/2-1:zmq-socket#toc11
    .. _PULL: http://api.zeromq.org/2-1:zmq-socket#toc13
    .. _DEALER: http://api.zeromq.org/2-1:zmq-socket#toc6
    """
    def __init__(
        self, 
        context, 
        server_node_name,
        server_address, 
        client_tag, 
        client_address,
        deliverator
    ):
        Greenlet.__init__(self)

        self._log = logging.getLogger("ResilientClient-%s" % (
            server_address, 
        ))

        self._context = context
        self._server_node_name = server_node_name
        self._server_address = server_address

        self._dealer_socket = None

        self._client_tag = client_tag
        self._client_address = client_address
        self._deliverator = deliverator

        self._send_queue = gevent.queue.Queue()
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

    @property
    def connected(self):
        return self._status == _status_connected

    @property
    def server_node_name(self):
        return self._server_node_name

    def test_current_status(self):
        """
        check for timeouts based on current state
        """
        self._lock.acquire()
        try:
            self._dispatch_table[self._status]()
        finally:
            self._lock.release()

        elapsed_time = time.time() - self._status_time
        return (_status_name[self._status], 
                elapsed_time, 
                self._send_queue.qsize()) 

    def _handle_status_disconnected(self):
        elapsed_time = time.time() - self._status_time 
        if elapsed_time < _handshake_retry_interval:
            return

        assert self._dealer_socket is None
        self._dealer_socket = self._context.socket(zmq.XREQ)
        self._dealer_socket.setsockopt(zmq.LINGER, 1000)
        self._log.debug("connecting to server")
        self._dealer_socket.connect(self._server_address)

        self._status = _status_handshaking
        self._log.info("status = %s" % (_status_name[self._status], ))

        message_control = {
            "message-type"      : "resilient-server-handshake",
            "message-id"        : uuid.uuid1().hex,
            "client-tag"        : self._client_tag,
            "client-address"    : self._client_address,
        }

        self.queue_message_for_send(message_control)

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

        message = message_format(ident=None, control=reply, body=None)
        self._deliverator.deliver_reply(message)

        # heave the message; we're heaving the whole request
        self._pending_message = None
        self._pending_message_start_time = None

    def _handle_status_handshaking(self):    

        # there's a race condition where we hit this check before the
        # handshake request gets sent. So we just return and hope
        # to hit it next time
        if self._pending_message is None:
            self._log.warn("_handle_status_handshaking with no pending message")
            return

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
        self._dealer_socket.close()
        self._dealer_socket = None

        self._status = _status_disconnected
        self._log.info("status = %s" % (_status_name[self._status], ))
        self._status_time = time.time()

    def join(self, timeout=3.0):
        self._log.debug("joining")
        if self._dealer_socket is not None:
            self._dealer_socket.close()
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def queue_message_for_send(self, message_control, data=None):

        if not "message-id" in message_control:
            message_control["message-id"] = uuid.uuid1().hex

        message_id = message_control["message-id"]
        delivery_channel = self._deliverator.add_request(message_id)

        message = message_format(
            ident=None, control=message_control, body=data
        )

        self._send_queue.put(message)

        return delivery_channel

    def _run(self):
        while True:

            # loop (with delay) until we are connected
            if self._dealer_socket is None:
                self._log.warn("not connected (%s), waiting %s seconds" % (
                    _status_name[self._status], _connect_delay
                ))
                gevent.sleep(_connect_delay)
                continue

            # block until we get a message to send
            message_to_send = self._send_queue.get()

            self._lock.acquire()

            self._pending_message = message_to_send
            self._pending_message_start_time = time.time()

            self._send_message(message_to_send)

            self._lock.release()

            # block until we get an ack
            message = self._dealer_socket.recv_json()

            self._lock.acquire()

            message_type = self._pending_message.control["message-type"]
            self._log.debug("received ack: %s %s" % (
                message_type, message["message-id"],
            ))
            self._last_successful_ack_time = time.time()

            # if we got an ack to a handshake request, we are connected
            if message_type == "resilient-server-handshake":
                assert self._status == _status_handshaking, self._status
                self._deliverator.discard_delivery_channel(
                    message["message-id"]
                )
                self._status = _status_connected
                self._log.info("status = %s" % (_status_name[self._status], ))
                self._status_time = time.time()                

            self._pending_message = None
            self._pending_message_start_time = None

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

    def __str__(self):
        return "ResilientClient-%s" % (self._server_node_name, )

