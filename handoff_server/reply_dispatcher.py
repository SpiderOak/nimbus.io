# -*- coding: utf-8 -*-
"""
reply_dispatcher.py

a class that dispatches queued replies to messages
"""
import logging
import pickle

from gevent.greenlet import Greenlet

from handoff_server.pending_handoffs import PendingHandoffs

_min_handoff_replies = 9

class ReplyDispatcher(Greenlet):
    """
    zmq_context
        zeromq context

    interaction_pool
        pool of database connections

    event_push_client
        client for event notification

    reply_queue
        queue for incoming reply messages

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 interaction_pool, 
                 event_push_client,
                 reply_queue, 
                 halt_event):
        Greenlet.__init__(self)
        self._name = "ReplyDispatcher"

        self._log = logging.getLogger(self._name)

        self._interaction_pool = interaction_pool
        self._event_push_client = event_push_client
        self._reply_queue = reply_queue
        self._halt_event = halt_event

        self._pending_handoffs = PendingHandoffs()
        self._handoff_reply_count = 0
        self._handoff_replies_already_seen = set()

        self._dispatch_table = {
            "request-handoffs-reply" : self._handle_request_handoffs_reply,
        }

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
        while not self._halt_event.is_set():
            message = self._reply_queue.get()
            if not message.control["message-type"] in self._dispatch_table:
                error_message = "unidentified message-type {0}".format(
                    message.control)
                self._event_push_client.error("handoff-reply-error",
                                              error_message)
                self._log.error(error_message)
                continue

            try:
                self._dispatch_table[message.control["message-type"]](
                    message)
            except Exception, instance:
                error_message = "exception during {0} {1}".format(
                    message.control["message-type"], str(instance))
                self._event_push_client.exception("handoff-reply-error",
                                              error_message)
                self._log.exception(error_message)
                continue

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

        segment_count  = 0
        already_seen_count = 0
        for segment_row in data_dict["segment"]:
            cache_key = (segment_row["id"], source_node_name, )
            if cache_key in self._handoff_replies_already_seen:
                already_seen_count += 1
                continue
            self._handoff_replies_already_seen.add(cache_key)
            self._pending_handoffs.push(segment_row, source_node_name)
            segment_count += 1
        if already_seen_count > 0:
            self._log.info("ignored {0} handoff segments: already seen".format(
                already_seen_count))
        if segment_count > 0:
            self._log.info("pushed {0} handoff segments".format(segment_count))
        
        self._handoff_reply_count += 1
        self._log.info("{0} handoff replies out of {1}".format(
            self._handoff_reply_count, _min_handoff_replies))

        if self._handoff_reply_count == _min_handoff_replies:
            self._log.info("starting segment handoffs")

