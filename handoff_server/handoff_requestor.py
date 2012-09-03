# -*- coding: utf-8 -*-
"""
handoff_requestor.py

a class that sends a 'request-handoffs' message to all active handoff
servers
"""
import logging
import os
import time
import uuid

import gevent
from gevent.greenlet import Greenlet
from gevent_zeromq import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import create_timestamp

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_timeout_seconds = 15.0

class HandoffRequestor(Greenlet):
    """
    zmq_context
        zeromq context

    addresses
        the address of every handoff_server except ourselves

    local_node_id
        the database id of the node row for our local node

    client_tag
        A unique identifier for our client, to be included in every message

    client_address
        the address our socket binds to. Sent to the remote server in every
        message

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 zmq_context, 
                 addresses, 
                 local_node_id, 
                 client_tag,
                 client_address,
                 halt_event):
        Greenlet.__init__(self)
        self._name = "HandoffRequestor"
        self._log = logging.getLogger(self._name)

        self._req_sockets = list()
        for address in addresses:
            req_socket = zmq_context.socket(zmq.REQ)

            # we need a valid path for IPC sockets
            if address.startswith("ipc://"):
                prepare_ipc_path(address)

            self._log.info("connecting to {0}".format(address))
            req_socket.connect(address)

            self._req_sockets.append(req_socket)

        self._local_node_id = local_node_id
        self._client_tag = client_tag
        self._client_address = client_address
        self._halt_event = halt_event

    def __str__(self):
        return self._name

    def _run(self):
        self._log.debug("sending handoff requests")

        message = {
            "message-type"              : "request-handoffs",
            "message-id"                : uuid.uuid1().hex,
            "client-tag"                : self._client_tag,
            "client-address"            : self._client_address,
            "request-timestamp-repr"    : repr(create_timestamp()),
            "node-name"                 : _local_node_name,
            "node-id"                   : self._local_node_id,
        }

        # send the message to everyone
        for req_socket in self._req_sockets:
            if self._halt_event.is_set():
                return
            req_socket.send_json(message)

        # wait for ack
        for index, req_socket in enumerate(self._req_sockets):
            start_time = time.time()
            reply = None
            while not self._halt_event.is_set():
                try:
                    reply = req_socket.recv_json(zmq.NOBLOCK)
                except zmq.ZMQError, instance:
                    if instance.errno == zmq.EAGAIN:
                        elapsed_time = time.time() - start_time
                        if elapsed_time < _timeout_seconds:
                            self._halt_event.wait(1.0)
                            continue
                        break
                    raise
                else:
                    break
            req_socket.close()
            if self._halt_event.is_set():
                return
            if reply is None:
                self._log.error(
                    "handoff_server #{0} has not acknowledged".format(index))
                continue
                    
            assert reply["accepted"] 

        self._req_sockets = list()

