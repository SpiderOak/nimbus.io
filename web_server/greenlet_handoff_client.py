# -*- coding: utf-8 -*-
"""
greenlet_client.py

a class that manages a zeromq DEALER (aka XREQ) socket as a client,
to a resilient server, and which hands off undeliverable messages
"""
from tools.greenlet_resilient_client import GreenletResilientClient

class GreenletHandoffClient(GreenletResilientClient):
    """
    a class that manages a zeromq DEALER (aka XREQ) socket as a client
    """
    def __init__(
        self, 
        context, 
        pollster,
        server_node_name,
        server_address, 
        client_tag, 
        client_address,
        deliverator,
        handoff_queues,
        handoff_queue_preference
    ):
        GreenletResilientClient.__init__(
            self,
            context, 
            pollster,
            server_node_name,
            server_address, 
            client_tag, 
            client_address,
            deliverator,
        )
        self._handoff_queues = handoff_queues
        self._handoff_queue_preference = handoff_queue_preference

    def __str__(self):
        return "HandoffClient-%s" % (self._server_node_name, )

