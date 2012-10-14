# -*- coding: utf-8 -*-
"""
handoff_requestor.py

a class that sends a 'request-handoffs' message to all active handoff
servers
"""
import logging
import os
import pickle
import uuid

import gevent
from gevent.greenlet import Greenlet

from tools.data_definitions import create_timestamp

from handoff_server.req_socket import ReqSocket, ReqSocketReplyTimeOut

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

class HandoffRequestor(Greenlet):
    """
    zmq_context
        zeromq context

    event_push_client
        client for event notification

    addresses
        the address of every handoff_server except ourselves

    local_node_id
        the database id of the node row for our local node

    client_tag
        A unique identifier for our client, to be included in every message

    client_address
        the address our socket binds to. Sent to the remote server in every
        message

    pending_handoffs
        a queue of pending handoffs

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 zmq_context, 
                 event_push_client,
                 addresses, 
                 local_node_id, 
                 client_tag,
                 client_address,
                 pending_handoffs,
                 halt_event):
        Greenlet.__init__(self)
        self._name = "HandoffRequestor"
        self._log = logging.getLogger(self._name)

        self._zmq_context = zmq_context
        self._event_push_client = event_push_client
        self._addresses = addresses
        self._local_node_id = local_node_id
        self._client_tag = client_tag
        self._client_address = client_address
        self._pending_handoffs = pending_handoffs
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
        req_sockets = list()
        for address in self._addresses:
            req_socket = ReqSocket(self._zmq_context,
                                   address, 
                                   self._client_tag, 
                                   self._client_address,
                                   self._halt_event)
            req_sockets.append(req_socket)
            if self._halt_event.is_set():
                break
            self._log.debug("sending to {0}".format(address))
            req_socket.send(message)

        if self._halt_event.is_set():
            for req_socket in req_sockets:
                req_socket.close()
            return

        for address, req_socket in zip(self._addresses, req_sockets):
            if self._halt_event.is_set():
                break
            self._log.debug("waiting for reply from {0}".format(address))
            try:
                message = req_socket.wait_for_reply()
            except ReqSocketReplyTimeOut, instance:
                self._log.error("timeout waiting reply {0} {1}".format(
                    str(req_socket), str(instance)))
                continue
            if message is not None:
                self._handle_request_handoffs_reply(message)

        for address, req_socket in zip(self._addresses, req_sockets):
            self._log.debug("closing {0}".format(address))
            req_socket.close()

    def _handle_request_handoffs_reply(self, message):
        self._log.info(
            "node {0} {1} conjoined-count={2} segment-count={3} {4}".format(
            message.control["node-name"], 
            message.control["result"], 
            message.control["conjoined-count"], 
            message.control["segment-count"], 
            message.control["request-timestamp-repr"]))

        if message.control["result"] != "success":
            error_message = \
                "request-handoffs failed on node {0} {1} {2}".format(
                message.control["node-name"], 
                message.control["result"], 
                message.control["error-message"])
            self._event_push_client.error("handoff-reply-error",
                                          error_message)
            self._log.error(error_message)
            return

        if message.control["conjoined-count"] == 0 and \
            message.control["segment-count"] == 0:
            self._log.info("no handoffs from {0}".format(
                message.control["node-name"]))
            return

        try:
            data_dict = pickle.loads(message.body)
        except Exception, instance:
            error_message = "unable to load handoffs from {0} {1}".format(
                message.control["node-name"], str(instance))
            self._event_push_client.exception("handoff-reply-error",
                                              error_message)
            self._log.exception(error_message)
            return

        source_node_name = message.control["node-name"]

        for segment_row in data_dict["segment"]:
            self._pending_handoffs.push(segment_row, source_node_name)
        self._log.info("pushed {0} handoff segments".format(
            len(data_dict["segment"])))

