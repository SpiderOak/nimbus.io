# -*- coding: utf-8 -*-
"""
request_dispatcher.py

a class that dispatches queued request messages
"""
import logging

from gevent.greenlet import Greenlet
from gevent.pool import Group

from gevent_zeromq import zmq

from tools.unhandled_greenlet_exception import \
    unhandled_greenlet_exception_closure

from handoff_server.handoffs_for_node import HandoffsForNode 

class RequestDispatcher(Greenlet):
    """
    zmq_context
        zeromq context

    interaction_pool
        pool of database connections

    event_push_client
        client for event notification

    request_queue
        queue for incoming request messages

    push_clent_dict
        dict mkeys by address of PUSH clients for responsing to requests

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 zmq_context,
                 interaction_pool, 
                 event_push_client,
                 request_queue, 
                 push_client_dict,
                 halt_event):
        Greenlet.__init__(self)
        self._name = "RequestDispatcher"

        self._log = logging.getLogger(self._name)

        self._zmq_context = zmq_context
        self._interaction_pool = interaction_pool
        self._event_push_client = event_push_client
        self._request_queue = request_queue
        self._push_client_dict = push_client_dict
        self._halt_event = halt_event
        self._active_greenlets = Group()

    def __str__(self):
        return self._name

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        self._active_greenlets.join(timeout)
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        while not self._halt_event.is_set():
            message = self._request_queue.get()
            if message.control["message-type"] != "request-handoffs":
                error_message = "unidentified message-type {0}".format(
                    message.control)
                self._event_push_client.error("handoff-request-error",
                                              error_message)
                self._log.error(error_message)
                continue

            if message.control["client-address"] not in self._push_client_dict:
                push_client = self._zmq_context.socket(zmq.PUSH)
                self._log.info("connecting to {0}".format(
                    message.control["client-address"]))
                push_client.connect(message.control["client-address"])
                self._push_client_dict[message.control["client-address"]] = \
                    push_client

            push_client = \
                self._push_client_dict[message.control["client-address"]]

            handoffs_greenlet = HandoffsForNode(self._interaction_pool,
                                                self._event_push_client,
                                                message,
                                                push_client,
                                                self._halt_event)
            handoffs_greenlet.link_exception(
                unhandled_greenlet_exception_closure(self._event_push_client))
            handoffs_greenlet.start()
            self._active_greenlets.add(handoffs_greenlet)

