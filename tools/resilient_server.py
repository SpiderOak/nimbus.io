# -*- coding: utf-8 -*-
"""
resilient_server.py

a class that manages a ROUTER (aka XREP) socket and some PUSH clients 
as a resilient server
"""
from collections import namedtuple
import logging

import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.push_client import PUSHClient

# our internal message format
_message_format = namedtuple("Message", "ident control body")

class ResilientServer(object):
    """
    a class that manages an ROUTER (aka XREP) socket and some PUSH clients 
    as a resilient server
    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("ResilientServer-%s" % (address, ))

        self._context = context
        self._router_socket = context.socket(zmq.XREP)
        self._router_socket.setsockopt(zmq.LINGER, 1000)

        # a server can bind to multiple zeromq addresses
        if type(address) in [list, tuple, ]:
            addresses = address
        else:
            addresses = [address, ]

        for bind_address in addresses:
            # we need a valid path for IPC sockets
            if bind_address.startswith("ipc://"):
                prepare_ipc_path(bind_address)

            self._log.debug("binding to %s" % (bind_address, ))
            self._router_socket.bind(bind_address)

        self._receive_queue = receive_queue

        self._dispatch_table = {
            "resilient-server-handshake" : \
                self._handle_resilient_server_handshake,
            "resilient-server-signoff" : \
                self._handle_resilient_server_signoff,
        }

        self._active_clients = dict()

    def register(self, pollster):
        pollster.register_read(self._router_socket, self._pollster_callback)

    def unregister(self, pollster):
        pollster.unregister(self._router_socket)

    def close(self):
        self._router_socket.close()
        for client in self._active_clients.values():
            client.close()

    def send_reply(self, message, data=None):
        """
        send a reply to the pull server of the requestor
        """
        try:
            client = self._active_clients[message["client-tag"]]
        except KeyError:
            self._log.error("send: No active client %s message discarded" % (
                message["client-tag"]
            ))
        else:
            client.send(message, data)

    def _pollster_callback(self, _active_socket, readable, writable):
        
        # assume we are readable, because we are only registered for read
        assert readable
        message = self._receive_message()      

        # if we get None, that means the socket would have blocked
        # go back and wait for more
        if message is None:
            return

        # we handle our own message traffic (i.e. resilient client handshakes
        # and signoffs).
        # otherwise, feed message into the receive queue to be handled
        # elsewhere
        if message.control["message-type"] in self._dispatch_table:
            self._dispatch_table[message.control["message-type"]](
                message.control, message.body
            )
            self._send_ack(
                message.control["message-type"],
                message.ident, 
                message.control["message-id"])
        elif not "client-tag" in message.control:
            self._log.error("receive: invalid message '%s'" % (
                message.control, 
            ))
        else:
            if message.control["client-tag"] in self._active_clients:
                self._receive_queue.append((message.control, message.body, ))
                self._send_ack(
                    message.control["message-type"],
                    message.ident, 
                    message.control["message-id"]
                )
            else:
                self._log.error(
                    "receive: No active client %s message discarded" % (
                        message.control["client-tag"]
                    )
                )

    def _send_ack(self, incoming_type, message_ident, message_id):            
        # send an immediate ack, zeromq sockets seem to always be writable
        self._log.debug("sending ack: %s %s" % (incoming_type, message_id, ))
        ack_message = {
            "message-type" : "resilient-server-ack",
            "message-id"   : message_id,
            "incoming-type": incoming_type,
        }
        self._router_socket.send(message_ident, zmq.SNDMORE)
        self._router_socket.send_json(ack_message)

    def _receive_message(self):
        try:
            ident = self._router_socket.recv(zmq.NOBLOCK)        
        except zmq.ZMQError, instance:
            if instance.errno == zmq.EAGAIN:
                self._log.warn("socket would have blocked")
                return None
            raise

        assert self._router_socket.rcvmore(), \
            "Unexpected missing message control part."
        control = self._router_socket.recv_json()

        body = []
        while self._router_socket.rcvmore():
            body.append(self._router_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return _message_format(ident=ident, control=control, body=body)

    def _handle_resilient_server_handshake(self, message, _data):
        log = logging.getLogger("_handle_resilient_server_handshake")
        log.info("%(client-tag)s %(client-address)s" % message)

        if message["client-tag"] in self._active_clients:
            log.warn("replacing existing client %(client-tag)s" % message) 
            self._active_clients[message["client-tag"]].close()

        self._active_clients[message["client-tag"]] = PUSHClient(
            self._context,
            message["client-address"]
        )
        
    def _handle_resilient_server_signoff(self, message, _data):
        log = logging.getLogger("_handle_resilient_server_signoff")
        try:
            client = self._active_clients.pop(message["client-tag"])
        except KeyError:
            log.info("no such client: %(client-tag)s" % message)
        else:
            log.info("closing client: %(client-tag)s" % message)
            client.close()


