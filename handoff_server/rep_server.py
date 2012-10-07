# -*- coding: utf-8 -*-
"""
rep_server.py

a class that manages a zeromq REP socket as a server,
"""
import logging
import os
import pickle

from  gevent.greenlet import Greenlet
from gevent_zeromq import zmq

from tools.zeromq_util import prepare_ipc_path
from tools.data_definitions import message_format

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

def _retrieve_conjoined_handoffs_for_node(interaction_pool, node_id):
    query = """
        select * from nimbusio_node.conjoined
        where handoff_node_id = %s
        order by unified_id
    """
    async_result = \
        interaction_pool.run(interaction=query, 
                             interaction_args=[node_id, ],
                             pool=_local_node_name) 
    result_list = async_result.get()

    conjoined_row_list = list()
    for row in result_list:
        # bytea columns come out of the database as buffer objects
        if row["combined_hash"] is not None: 
            row["combined_hash"] = str(row["combined_hash"])
        # row is of type psycopg2.extras.RealDictRow
        # we want something we can pickle
        conjoined_row_list.append(dict(row.items()))

    return conjoined_row_list

def _retrieve_segment_handoffs_for_node(interaction_pool, node_id):
    query = """
        select * from nimbusio_node.segment 
        where handoff_node_id = %s
        order by timestamp desc
    """
    async_result = \
        interaction_pool.run(interaction=query, 
                             interaction_args=[node_id, ],
                             pool=_local_node_name) 
    result_list = async_result.get()

    segment_row_list = list()
    for row in result_list:
        # bytea columns come out of the database as buffer objects
        if row["file_hash"] is not None: 
            row["file_hash"] = str(row["file_hash"])
        # row is of type psycopg2.extras.RealDictRow
        # we want something we can pickle
        segment_row_list.append(dict(row.items()))

    return segment_row_list

class REPServer(Greenlet):
    """
    zmq_context
        zeromq context

    interaction_pool
        pool of database connections

    event_push_client
        client for event notification

    address
        the address our socket binds to. 

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 zmq_context, 
                 interaction_pool, 
                 event_push_client, 
                 address, 
                 halt_event):
        Greenlet.__init__(self)
        self._name = "REPServer"

        self._log = logging.getLogger(self._name)

        self._interaction_pool = interaction_pool
        self._event_push_client = event_push_client

        # we need a valid path for IPC sockets
        if address.startswith("ipc://"):
            prepare_ipc_path(address)

        self._rep_socket = zmq_context.socket(zmq.REP)
        self._log.debug("binding to {0}".format(address))
        self._rep_socket.bind(address)

        self._halt_event = halt_event
        self._ping_count = 0

        self._dispatch_table = {
            "ping"              : self._handle_ping,
            "request-handoffs"  : self._handle_request_handoffs, }

    def __str__(self):
        return self._name

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        self._rep_socket.close()
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        while not self._halt_event.is_set():
            try:
                control = self._rep_socket.recv_json(zmq.NOBLOCK)
            except zmq.ZMQError, instance:
                if instance.errno == zmq.EAGAIN:
                    self._halt_event.wait(1.0)
                    continue
                raise
            body = []
            while self._rep_socket.rcvmore:
                body.append(self._rep_socket.recv())

            if len(body) == 0:
                body = None
            elif len(body) == 1:
                body = body[0]

            message = message_format(ident=None, control=control, body=body)
            try:
                self._dispatch_table[message.control["message-type"]](message)
            except KeyError:
                self._log.error("unknown message {0}".format(message.control))

    def _handle_ping(self, message):
        # ack is sufficient rely to ping
        self._ping_count += 1
        if self._ping_count % 100 == 0:
            self._log.debug("{0} pings".format(self._ping_count))

        ack_message = {
            "message-type" : "resilient-server-ack",
            "message-id"   : message.control["message-id"],
            "incoming-type": message.control["message-type"],
            "accepted"     : True
        }

        self._rep_socket.send_json(ack_message)


    def _handle_request_handoffs(self, message):
        self._log.debug("node {0} {1}".format(
            message.control["node-name"], 
            message.control["request-timestamp-repr"]))

        reply = {
            "message-type"          : "request-handoffs-reply",
            "client-tag"            : message.control["client-tag"],
            "message-id"            : message.control["message-id"],
            "request-timestamp-repr": \
                message.control["request-timestamp-repr"],
            "node-name"             : _local_node_name,
            "conjoined-count"       : None,
            "segment-count"         : None,
            "result"                : None,
            "error-message"         : None,
        }

        node_id = message.control["node-id"]
        try:
            conjoined_rows = \
                _retrieve_conjoined_handoffs_for_node(self._interaction_pool,
                                                      node_id)
            segment_rows = \
                _retrieve_segment_handoffs_for_node(self._interaction_pool, 
                                                    node_id)
        except Exception, instance:
            self._log.exception(str(instance))
            self._event_push_client.exception(
                "_retrieve_handoffs_for_node", str(instance))  
            reply["result"] = "exception"
            reply["error-message"] = str(instance)
            self._rep_socket.send_json(reply)
            return

        reply["conjoined-count"] = len(conjoined_rows)
        reply["segment-count"] = len(segment_rows)
        self._log.debug("found {0} conjoined, {0} segments".format(
            reply["conjoined-count"], reply["segment-count"]))

        data_dict = dict()
        data_dict["conjoined"] = conjoined_rows
        data_dict["segment"] = segment_rows

        try:
            data = pickle.dumps(data_dict)
        except Exception, instance:
            error_message = "{0}".format(instance)
            self._log.exception(error_message)
            self._event_push_client.exception("pickle handoffs", error_message)
            reply["result"] = "exception"
            reply["error-message"] = error_message
            self._rep_socket.send_json(reply)
        else:
            reply["result"] = "success"
            self._rep_socket.send_json(reply, zmq.SNDMORE)
            self._rep_socket.send(data)

