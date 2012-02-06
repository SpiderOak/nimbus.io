# -*- coding: utf-8 -*-
"""
handoff_client.py

class DataWriterHandoffClient

This class mimics a single ResilientClient while wrapping two clients
to perform hinted handoff for a node which is down.

This is intended to fool the DataWriter into believing it is writing
to a normal client.
"""
import logging

import gevent
from gevent.queue import Queue

from tools.data_definitions import message_format

_data_writer_timeout = 30.0

class DataWriterHandoffClient(object):
    """
    dest_node_name
        the name of the (ultimate) destination node

    backup_clients
        a list of two GreenletResilientClient objects that will receive
        archive messages to back them up for later handoff

    This class mimics a single GreenletResilientClient. 
    
    When doing an archive, if the clients for a node is disconnected, 
    the web_server replaces it with a DataWriterHandoffClient which
    replicates the archive messages to two other nodes.

    These replicated messages will later be handed off to the destination node
    by the handoff server,
    """
    def __init__( self, dest_node_name, backup_clients):
        self._log = logging.getLogger("HandoffClient-%s" % (dest_node_name,))
        self._log.info("handing off to %s" % (
            ", ".join([str(c) for c in backup_clients]), 
        ))
        self._dest_node_name = dest_node_name
        self._backup_clients = backup_clients
        self._handoff_message = dict()
   
    def queue_message_for_send(self, message, data=None):
        """
        accept a message as if we were a single client, but pass it on to our
        two internal backup clients.
        """
        completion_channel = Queue(maxsize=1)

        gevent.spawn(self._complete_handoff, message, data, completion_channel)

        return completion_channel

    def _complete_handoff(self, message, data, completion_channel):
        # hand off the message, 
        if "handoff-node-name" in message:
            assert message["handoff-node-name"] is None, message
            message["handoff-node-name"] = self._dest_node_name

        data_writer_greenlets = [
            # queue a copy of the message, so each gets a different message-id
            gevent.spawn(
                self._hand_off_to_one_data_writer, client, message.copy(), data
            ) \
            for client in self._backup_clients
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
                message = message_format(ident=None, control=reply, body=None)
                completion_channel.put((message.control, message.body, ))
                return

        # if we made it here all the handoffs succeeded, 
        # notify the sender that we are ok so far
        # by sending one of the data_writer_replies 
        reply = data_writer_replies[0]
        message = message_format(ident=None, control=reply, body=None)
        completion_channel.put((message.control, message.body, ))

    def _hand_off_to_one_data_writer(self, client, message, data):
        channel = client.queue_message_for_send(message, data)
        reply, _data = channel.get()
        return reply

