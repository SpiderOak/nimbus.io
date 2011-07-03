# -*- coding: utf-8 -*-
"""
handoff_client.py

class DataWriterHandoffClient

This class mimics a single ResilientClient while wrapping two clients
to perform hinted handoff for a node which is down.

This is intended to fool the DataWriter into believing it is writing
to a normal client.
"""
from collections import namedtuple
import logging

import gevent
from gevent.queue import Queue

_message_format = namedtuple("Message", "control body")
_data_writer_timeout = 30.0

class DataWriterHandoffClient(object):
    """
    This class mimics a single ResilientClient while wrapping two clients
    to perform hinted handoff for a node which is down.
    """
    def __init__( self, dest_node_name, dest_node_id, resilient_clients):
        self._log = logging.getLogger("HandoffClient-%s" % (dest_node_name,))
        self._log.info("handing off to %s" % (
            ", ".join([str(c) for c in resilient_clients]), 
        ))
        self._dest_node_id = dest_node_id
        self._resilient_clients = resilient_clients
        self._handoff_message = dict()
   
    def queue_message_for_send(self, message, data=None):
        completion_channel = Queue(maxsize=1)

        gevent.spawn(self._complete_handoff, message, data, completion_channel)

        return completion_channel

    def _complete_handoff(self, message, data, completion_channel):
        # hand off the message, 
        if "handoff-node-id" in message:
            assert message["handoff-node-id"] is None, message
            message["handoff-node-id"] = self._dest_node_id

        data_writer_greenlets = [
            # queue a copy of the message, so each gets a different message-id
            gevent.spawn(
                self._hand_off_to_one_data_writer, client, message.copy(), data
            ) \
            for client in self._resilient_clients
        ]
    
        # get all replies from the actual clients  
        gevent.joinall(data_writer_greenlets, timeout=_data_writer_timeout)
        all_ready = all([g.ready() for g in data_writer_greenlets])
        if not all_ready:
            self._log.error("Not all data writer greenlets finished")
        assert all_ready
        data_writer_replies = [g.value for g in data_writer_greenlets]

        # if any data writer has failed, we have failed
        for data_writer_reply in data_writer_replies:
            if data_writer_reply["result"] != "success":
                reply = {
                   "message-type"   : "handoff-failure",
                   "result"         : "handoff-failure",
                   "error-message"  : data_writer_reply["error-message"],
                }
                message = _message_format(control=reply, body=None)
                completion_channel.put(message)
                return

        # if we made it here all the handoffs succeeded, 
        # notify the sender that we are ok so far
        # by sending one of the data_writer_replies 
        reply = data_writer_replies[0]
        message = _message_format(control=reply, body=None)
        completion_channel.put(message)

    def _hand_off_to_one_data_writer(self, client, message, data):
        channel = client.queue_message_for_send(message, data)
        reply, _data = channel.get()
        return reply

