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
import gevent
from gevent_zeromq import zmq

from tools.data_definitions import message_format

class ResilientClientError(Exception):
    pass

_polling_interval = 3.0
_ack_timeout = float(os.environ.get("NIMBUSIO_ACK_TIMEOUT", "60.0"))
_handshake_retry_interval = 60.0
_max_idle_time = 10 * 60.0
_reporting_interval = 60.0
_connect_delay = 60.0

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

    Each resilient client maintains its own DEALER_ socket **_self._dealer_socket**.

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

        self._client_tag = client_tag
        self._client_address = client_address
        self._deliverator = deliverator

        self._send_queue = gevent.queue.Queue()

        self._dealer_socket = None
        self.connected = False

    @property
    def server_node_name(self):
        return self._server_node_name

    @property
    def queue_size(self):
        return self._send_queue.qsize()

    def join(self, timeout=3.0):
        self._log.debug("joining")
        if self._dealer_socket is not None:
            self._dealer_socket.close()
            self._dealer_socket = None
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def queue_message_for_send(self, message_control, data=None):
        if not self.connected:
            raise ResilientClientError(
                "queue_message_for_send while not connected  %s" % (
                    message_control,
                )
            )

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

            assert not self.connected

            self._dealer_socket = self._context.socket(zmq.XREQ)
            self._dealer_socket.setsockopt(zmq.LINGER, 1000)
            self._log.debug("connecting to server")
            self._dealer_socket.connect(self._server_address)

            # send a handshake
            message_control = {
                "message-type"      : "resilient-server-handshake",
                "message-id"        : uuid.uuid1().hex,
                "client-tag"        : self._client_tag,
                "client-address"    : self._client_address,
            }
            self._dealer_socket.send_json(message_control)

            # wait for  an ack
            ack_reply = gevent.with_timeout(
                _ack_timeout, 
                self._dealer_socket.recv_json,
                timeout_value=None
            )
            if ack_reply is None:
                error_message = \
                    "timeout waiting handshake ack: retry {0} seconds".format(
                        _handshake_retry_interval
                    )
                self._log.error(error_message)
                self._dealer_socket.close()
                self._dealer_socket = None
                gevent.sleep(_handshake_retry_interval)
                continue

            self.connected = True

            while self.connected:

                # block until we get a message to send
                message_to_send = self._send_queue.get()

                self._send_message(message_to_send)

                # wait for  an ack
                ack_reply = gevent.with_timeout(
                    _ack_timeout, 
                    self._dealer_socket.recv_json,
                    timeout_value=None
                )
                if ack_reply is None:
                    error_message = \
                        "timeout waiting ack: treating as disconnect"
                    self._log.error(error_message)
                    self._dealer_socket.close()
                    self._dealer_socket = None

                    self.connected = False

                    self._deliver_failure_reply(message_to_send)

                    gevent.sleep(_handshake_retry_interval)
                    break

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

    def _deliver_failure_reply(self, message_to_send):
        """
        deliver a failure reply to everyone waiting for this socket
        """
        work_message = message_to_send
        while work_message is not None:

            reply = {
                "message-type"  : "ack-timeout-reply",
                "message-id"    : work_message.control["message-id"],
                "result"        : "ack timeout",
                "error-message" : "timeout waiting ack: treating as disconnect"
            }

            message = message_format(ident=None, control=reply, body=None)
            self._deliverator.deliver_reply(message)

            try:
                work_message = self._send_queue.get_nowait()
            except gevent.queue.Empty:
                work_message = None

    def __str__(self):
        return "ResilientClient-%s" % (self._server_node_name, )

