# -*- coding: utf-8 -*-
"""
resilient_server.py

a class that manages a REP socket and some PUSH clients 
as a resilient server
"""
import logging

import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.push_client import PUSHClient
from tools.data_definitions import message_format

class ResilientServer(object):
    """
    a class that manages a REP socket and some PUSH clients 
    as a resilient serve.

    The resilient server receives messages from resilient clients over a
    REP socket and sends replies using PUSH clients.

    """
    def __init__(self, context, address, receive_queue):
        self._log = logging.getLogger("ResilientServer-%s" % (address, ))

        self._context = context
        self._rep_socket = context.socket(zmq.REP)
        self._rep_socket.setsockopt(zmq.LINGER, 1000)

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
            self._rep_socket.bind(bind_address)

        self._receive_queue = receive_queue

        self._dispatch_table = {
            "ping" : \
                self._handle_ping,
            "resilient-server-handshake" : \
                self._handle_resilient_server_handshake,
            "resilient-server-signoff" : \
                self._handle_resilient_server_signoff,
        }

        self._active_clients = dict()

    def register(self, pollster):
        """
        resiter ourselves with the pollster for reads
        """
        pollster.register_read(self._rep_socket, self.pollster_callback)

    def unregister(self, pollster):
        """
        unregister from the polster
        """
        pollster.unregister(self._rep_socket)

    def close(self):
        """
        close out ROUTER socket and and all the PUSH clients we are holding
        """
        self._rep_socket.close()
        for client in self._active_clients.values():
            client.close()

    def send_reply(self, message, data=None):
        """
        use the appropriate PUSH client (identified by 'client-tag') to send 
        a reply to the PULL server of the requestor
        """
        log = logging.getLogger("send_reply")
        if message["client-tag"] not in self._active_clients:
            log.info("creating PUSHClient {0} {1}".format(
                message["client-tag"], message["client-address"]))
            self._active_clients[message["client-tag"]] = PUSHClient(
                self._context,
                message["client-address"]
            )

        client = self._active_clients[message["client-tag"]]
        client.send(message, data)

    def pollster_callback(self, _active_socket, readable, writable):
        """
        when our ROUTER socket becomes readable, read a message from it

        we handle handshake and signoff messages from resilient clients

        for all other messages, 
         * we send an immediate ack reply over the DEALER socket 
         * we place the message in the receive queue
         * the message handler will eventually PUSH a detail reply to the
           resilient client's PULL server.

        """
        
        # assume we are readable, because we are only registered for read
        assert readable
        message = self._receive_message()      

        ack_message = {
            "message-type" : "resilient-server-ack",
            "message-id"   : message.control["message-id"],
            "incoming-type": message.control["message-type"],
            "accepted"     : None
        }

        if message.control["message-type"] in self._dispatch_table:
            self._dispatch_table[message.control["message-type"]](
                message.control, message.body
            )
        else:
            self._receive_queue.append((message.control, message.body, ))
        ack_message["accepted"] = True

        self._rep_socket.send_json(ack_message)

    def _receive_message(self):
        control = self._rep_socket.recv_json()

        body = []
        while self._rep_socket.rcvmore:
            body.append(self._rep_socket.recv())

        # 2011-04-06 dougfort -- if someone is expecting a list and we only get
        # one segment, they are going to have to deal with it.
        if len(body) == 0:
            body = None
        elif len(body) == 1:
            body = body[0]

        return message_format(ident=None, control=control, body=body)

    def _handle_ping(self, message, _data):
        log = logging.getLogger("_handle_ping")
        log.debug("{message-id}".format(**message))

    def _handle_resilient_server_handshake(self, message, _data):
        log = logging.getLogger("_handle_resilient_server_handshake")
        log.debug("%(client-tag)s %(client-address)s" % message)

        if message["client-tag"] in self._active_clients:
            log.debug("replacing existing client %(client-tag)s" % message) 
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


