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

from tools.data_definitions import segment_status_final, \
        segment_status_tombstone, \
        create_priority

from handoff_server.forwarder_coroutine import forwarder_coroutine

_handoff_starter_polling_interval = float(os.environ.get(
    "NIMBUSIO_HANDOFF_STARTER_POLLING_INTERVAL", str(60.0)
))
_delay_interval = 1.0

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
        start a handoff for a segment
        """
        if halt_event.is_set():
            return

        if self._state["forwarder"] is not None:
            # run again later
            return [(self.run, time.time() + _delay_interval, )]

        self._log.info("%s pending handoffs" % (
            len(self._state["pending-handoffs"])
        ))

        try:
            segment_row, source_node_names = \
                    self._state["pending-handoffs"].pop()
        except IndexError:
            self._log.debug("no handoffs found")
            # run again, after the polling interval
            return [(self.run, self.next_run(), )]

        if segment_row.status == segment_status_final:
            self._start_forwarder_coroutine(segment_row, source_node_names)
            return [(self.run, time.time() + _delay_interval, )]

        if segment_row.status == segment_status_tombstone:
            self._send_delete_message(segment_row, source_node_names)
            return [(self.run, time.time(), )]

        self._log.error("unknown status '%s' (%s) %s part=%s" % (
            segment_row.status,
            segment_row.collection_id,
            segment_row.key,
            segment_row.conjoined_part
        ))
        return [(self.run, time.time(), )]

    def _start_forwarder_coroutine(self, segment_row, source_node_names):
        # we pick the first source name, because it's easy, and because,
        # since that source responded to us first, it might have better 
        # response
        # TODO: switch over to the second source on error
        source_node_name = source_node_names[0]
        reader_client = self._state["reader-client-dict"][source_node_name]

        description = "start handoff from %s to %s (%s) %r part=%s" % (
            source_node_name,
            self._local_node_name,
            segment_row.collection_id,
            segment_row.key,
            segment_row.conjoined_part
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
            self._state["node-name-dict"],
            segment_row, 
            source_node_names, 
            self._state["writer-client-dict"][self._local_node_name], 
            reader_client
        )
        self._state["forwarder"].next()

    def _send_delete_message(self, segment_row, source_node_names):
        source_node_name = source_node_names[0]
        message = {
            "message-type"          : "destroy-key",
            "priority"              : create_priority(),
            "collection-id"         : segment_row.collection_id,
            "key"                   : segment_row.key,
            "unified-id-to-delete"  : segment_row.file_tombstone_unified_id,
            "unified-id"            : segment_row.unified_id,
            "timestamp-repr"        : repr(segment_row.timestamp),
            "segment-num"           : segment_row.segment_num,
            "source-node-name"      : source_node_name,
            "handoff-node-name"     : None,
        }
        writer_client = \
                self._state["writer-client-dict"][self._local_node_name]

        writer_client.queue_message_for_send(message, data=None)

        self._state["active-deletes"][segment_row.unified_id] = \
                source_node_names

