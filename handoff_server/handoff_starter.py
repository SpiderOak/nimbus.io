# -*- coding: utf-8 -*-
"""
handoff_requestor.py

class HandoffStarter

A time queue action to start handoffs if any nodes have sent us a reply to
handoff-request 
"""
import logging
import os
import time
 
from handoff_server.forwarder_coroutine import forwarder_coroutine

_handoff_starter_polling_interval = float(os.environ.get(
    "NIMBUSIO_HANDOFF_STARTER_POLLING_INTERVAL", str(60.0)
))

class HandoffStarter(object):
    """
    A time queue action to start handoffs if any nodes have sent us a reply to
    handoff-request 
    """
    def __init__(self, state, local_node_name, event_push_client):
        self._log = logging.getLogger("HandoffStarter")
        self._state = state
        self._local_node_name = local_node_name
        self._event_push_client = event_push_client

    @classmethod
    def next_run(cls):
        return time.time() + _handoff_starter_polling_interval

    def run(self, halt_event):
        """
        start a forwarder coroutine for a segment
        """
        if halt_event.is_set():
            return

        if self._state["forwarder"] is not None:
            # run again, after the polling interval
            return [(self.run, self.next_run(), )]

        try:
            segment_row, source_node_names = \
                    self._state["pending-handoffs"].pop()
        except IndexError:
            self._log.debug("no handoffs found")
            # run again, after the polling interval
            return [(self.run, self.next_run(), )]

        # we pick the first source name, because it's easy, and because,
        # since that source responded to us first, it might have better 
        # response
        # TODO: switch over to the second source on error
        source_node_name = source_node_names[0]
        reader_client = self._state["reader-client-dict"][source_node_name]

        description = "start handoff from %s to %s (%s) %r" % (
            source_node_name,
            self._local_node_name,
            segment_row.collection_id,
            segment_row.key
        )
        self._log.info(description)

        self._state["event-push-client"].info(
            "handoff-start",
            description,
            backup_source=source_node_name,
            collection_id=segment_row.collection_id,
            key=segment_row.key,
            timestamp_repr=repr(segment_row.timestamp)
        )
        
        self._state["forwarder"] = forwarder_coroutine(
            segment_row, 
            source_node_names, 
            self._state["writer-client-dict"][self._local_node_name], 
            reader_client
        )
        self._state["forwarder"].next()

        # run again, after the polling interval
        return [(self.run, self.next_run(), )]
