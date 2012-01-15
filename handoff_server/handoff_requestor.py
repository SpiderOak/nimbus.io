# -*- coding: utf-8 -*-
"""
handoff_requestor.py

class HandoffRequestor

A time queue action to periodically query the other handoff servers to 
see if their node has any handoffs for us.
"""
import logging
import os
import time

from tools.data_definitions import create_timestamp

handoff_polling_interval = float(os.environ.get(
    "NIMBUSIO_HANDOFF_POLLING_INTERVAL", str(15.0 * 60.0)
))

class HandoffRequestor(object):
    """
    A time queue action to periodically query the other handoff servers to 
    see if their node has any handoffs for us.
    """
    def __init__(self, state, local_node_name):
        self._log = logging.getLogger("HandoffRequestor")
        self._state = state
        self._local_node_name = local_node_name
        self._local_node_id = state["node-id-dict"][local_node_name]

    @classmethod
    def next_run(cls):
        return time.time() + handoff_polling_interval

    def run(self, halt_event):
        """
        send 'request-handoffs' to all remote handoff servers
        """

        if halt_event.is_set():
            return

        self._log.debug("sending handoff requests")

        message = {
            "message-type"              : "request-handoffs",
            "request-timestamp-repr"    : repr(create_timestamp()),
            "node-name"                 : self._local_node_name,
            "node-id"                   : self._local_node_id,
        }

        for handoff_server_client in self._state["handoff-server-clients"]:
            handoff_server_client.queue_message_for_send(message)
        
        return [(self.run, self.next_run(), )]

