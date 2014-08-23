# -*- coding: utf-8 -*-
"""
resilient_client.py

a class that manages a zeromq REQ socket as a client,
to a resilient server
"""
import logging
import os
import uuid

from  gevent.greenlet import Greenlet
import gevent.queue
import gevent
import zmq

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
        The zeromq address of the REP socket of the server we connect to

    client_tag
        A unique identifier for our client, to be included in every message

    client_address
        the address our socket binds to. Sent to the remote server in every 
        message

    deliverator
        handle to the deliverator object

    GreenletResilientClient uses two zeromq patterns to maintain a connection
    to a resilient server.

    - request-reply_ for sending messages and for receiving acknowledgements
    - pipeline_ for receiving the actual replies to messages

    Each process maintains a single PULL socket for all of its resilient 
    clients. The address of this socket is the member **_client_address**.
    The client includes this in every message along with **_client_tag** which
    uniquely identifies the client to the server

    Each resilient client maintains its own REQ socket **_self._req_socket**.

    At startup the client sends a *handshake* message to the server. The client
    is not considered connected until it gets an ack from the handshake.

    Normal workflow:
    
    1. The client pops a message from **_send_queue**
    2. The client sends the message over the DEALER_ socket
    3. The client waits for and ack from the server. It does not send any
       other messages until it gets the ack.
    4. When the ack is received the client repeats from step 1.
    5. The actual reply from the server comes to the PULL_ socket and is
       handled outside the client

    """
    def __init__(
        self, 
        context, 
        server_node_name,
        server_address, 
        client_tag, 
        client_address,
        deliverator,
        connect_messages=list()
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

        # prime the send queue with messages to be sent as soon
        # as we connect
        for connect_message in connect_messages:
            if not "message-id" in connect_message:
                connect_message["message-id"] = uuid.uuid1().hex
            message = message_format(
                ident=None, control=connect_message, body=None
            )
            self._send_queue.put(message)

        self._req_socket = None
        self.connected = False

    @property
    def server_node_name(self):
        return self._server_node_name

    @property
    def queue_size(self):
        return self._send_queue.qsize()

    def join(self, timeout=3.0):
        self._log.debug("joining")
        if self._req_socket is not None:
            self._req_socket.close()
            self._req_socket = None
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def queue_message_for_send(self, message_control, data=None):
        """
        message control must be a dict 
        If the caller includes a 'message-id' key we will use it,
        otherwise, we will supply one.
        return: a gevent queue (zero size Queue) the the reply will
        be deliverd.
        """
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

    def queue_message_for_broadcast(self, message_control, data=None):
        """
        queue a message for send, but do not create a delivery channel:
        we are not expecting a reply
        """
        if not "message-id" in message_control:
            message_control["message-id"] = uuid.uuid1().hex

        message = message_format(
            ident=None, control=message_control, body=data
        )

        self._send_queue.put(message)

    def _run(self):
        while True:

            assert not self.connected

            self._req_socket = self._context.socket(zmq.REQ)
            self._req_socket.setsockopt(zmq.LINGER, 1000)
            self._log.debug("connecting to server")
            self._req_socket.connect(self._server_address)

            # send a handshake
            message_control = {
                "message-type"      : "resilient-server-handshake",
                "message-id"        : uuid.uuid1().hex,
                "client-tag"        : self._client_tag,
                "client-address"    : self._client_address,
            }
            self._req_socket.send_json(message_control)

            # wait for  an ack
            ack_reply = gevent.with_timeout(
                _ack_timeout, 
                self._req_socket.recv_json,
                timeout_value=None
            )
            if ack_reply is None:
                error_message = \
                    "timeout waiting handshake ack: retry {0} seconds".format(
                        _handshake_retry_interval
                    )
                self._log.error(error_message)
                self._req_socket.close()
                self._req_socket = None
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
                    self._req_socket.recv_json,
                    timeout_value=None
                )
                if ack_reply is None:
                    error_message = \
                        "timeout waiting ack: treating as disconnect"
                    self._log.error(error_message)
                    self._req_socket.close()
                    self._req_socket = None

                    self.connected = False

                    self._deliver_failure_reply(message_to_send)

                    gevent.sleep(_handshake_retry_interval)
                    break

    def _send_message(self, message):
        self._log.debug("sending message: %s" % (message.control, ))
        message.control["client-tag"] = self._client_tag
        message.control["client-address"] = self._client_address

        # don't send a zero size body 
        if type(message.body) not in [list, tuple, type(None), ]:
            if len(message.body) == 0:
                message = message._replace(body=None)
            else:
                message = message._replace(body=[message.body, ])

        if message.body is None:
            self._req_socket.send_json(message.control)
        else:
            self._req_socket.send_json(message.control, zmq.SNDMORE)
            for segment in message.body[:-1]:
                self._req_socket.send(segment, zmq.SNDMORE)
            self._req_socket.send(message.body[-1])

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

