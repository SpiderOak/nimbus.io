# -*- coding: utf-8 -*-
"""
handoffs_for_node.py

query thedatabase to locate handoffs for a specific node
"""
import logging
import os
import pickle

from gevent.greenlet import Greenlet

from gevent_zeromq import zmq

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
        conjoined_row_list.append(row)

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
        segment_row_list.append(row)

    return segment_row_list


class HandoffsForNode(Greenlet):
    """
    interaction_pool
        pool of database connections

    event_push_client
        client for event notification

    message
        the message requesting handoffs for a specific node

    push_clent
        PUSH client to send results to the node

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 interaction_pool, 
                 event_push_client,
                 message, 
                 push_client,
                 halt_event):
        Greenlet.__init__(self)
        self._name = "HandoffsForNode"

        self._log = logging.getLogger(self._name)

        self._interaction_pool = interaction_pool
        self._event_push_client = event_push_client
        self._message = message
        self._push_client = push_client
        self._halt_event = halt_event

    def __str__(self):
        return self._name

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        self._log.debug("node {0} {1}".format(
            self._message.control["node-name"], 
            self._message.control["request-timestamp-repr"]))

        reply = {
            "message-type"          : "request-handoffs-reply",
            "client-tag"            : self._message.control["client-tag"],
            "self._message-id"      : self._message.control["message-id"],
            "request-timestamp-repr": \
                self._message.control["request-timestamp-repr"],
            "node-name"             : _local_node_name,
            "conjoined-count"       : None,
            "segment-count"         : None,
            "result"                : None,
            "error-self._message"   : None,
        }

        node_id = self._message.control["node-id"]
        try:
            conjoined_rows = \
                _retrieve_conjoined_handoffs_for_node(self._interaction_pool,
                                                      node_id )
            segment_rows = \
                _retrieve_segment_handoffs_for_node(self._interaction_pool, 
                                                    node_id)
        except Exception, instance:
            self._log.exception(str(instance))
            self._event_push_client.exception(
                "_retrieve_handoffs_for_node", str(instance))  
            reply["result"] = "exception"
            reply["error-message"] = str(instance)
            self._push_client.send_json(reply)
            return

        reply["result"] = "success"

        reply["conjoined-count"] = len(conjoined_rows)
        reply["segment-count"] = len(segment_rows)
        self._log.debug("found {0} conjoined, {0} segments".format(
            reply["conjoined-count"], reply["segment-count"]))

        data_dict = dict()
        data_dict["conjoined"] = conjoined_rows
        data_dict["segment"] = segment_rows
        data = pickle.dumps(data_dict)
            
        self._push_client.send_json(reply, zmq.SNDMORE)
        self._push_client.send(data)

