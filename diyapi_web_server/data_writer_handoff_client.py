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
    def __init__(
        self,
        original_dest_node_name,
        resilient_clients, 
        handoff_client
    ):
        self._log = logging.getLogger("HandoffClient-%s" % (
            original_dest_node_name,
        ))
        self._log.info("handing off to %s" % (
            ", ".join([str(c) for c in resilient_clients]), 
        ))
        self._original_dest_node_name = original_dest_node_name
        self._resilient_clients = resilient_clients
        self._handoff_client = handoff_client
        self._handoff_message = dict()
        self._dispatch_table = {
            "archive-key-entire"    : self._handle_archive_key_entire,
            "archive-key-start"     : self._handle_archive_key_start,
            "archive-key-next"      : self._handle_archive_key_next,
            "archive-key-final"     : self._handle_archive_key_final,
            "destroy-key"           : self._handle_destroy_key,
        }
   
    def queue_message_for_send(self, message, data=None):
        completion_channel = Queue(maxsize=1)

        gevent.spawn(self._complete_handoff, message, data, completion_channel)

        return completion_channel

    def _complete_handoff(self, message, data, completion_channel):
        # hand off the message, 
        # at this stage we don't care what message it is
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
        # now we care what the message is
        try:
            completed = self._dispatch_table[message["message-type"]](message)
        except KeyError, instance:
            self._log.error("dispatch error %s %s" % (instance, message, ))
            return

        # if we're not done, notify the sender that we are ok so far
        # by sending one of the data_writer_replies 
        if not completed:
            reply = data_writer_replies[0]
            message = _message_format(control=reply, body=None)
            completion_channel.put(message)
            return

        # now the archive (or destroy) is fully backed up,
        # we must send the hint to the handoff server
        handoff_client_channel = self._handoff_client.queue_message_for_send(
            self._handoff_message, data=None
        )        

        # just pass on whatever reply the handoff server gave us
        handoff_reply, _data = handoff_client_channel.get()
        if handoff_reply["result"] != "success":
            reply = {
               "message-type"   : "handoff-failure",
               "result"         : handoff_reply["result"],
               "error-message"  : handoff_reply["error-message"],
            }
        else:
            reply = data_writer_replies[0]
        completion_channel.put(_message_format(control=reply, body=None))

    def _hand_off_to_one_data_writer(self, client, message, data):
        channel = client.queue_message_for_send(message, data)
        reply, _data = channel.get()
        return reply

    def _handle_archive_key_entire(self, message):
        assert len(self._handoff_message) == 0, self._handoff_message
        self._handoff_message["message-type"] = "hinted-handoff"
        self._handoff_message["dest-node-name"] = self._original_dest_node_name
        self._handoff_message["avatar-id"] = message["avatar-id"]
        self._handoff_message["timestamp-repr"] = message["timestamp-repr"]
        self._handoff_message["key"] = message["key"]
        self._handoff_message["segment-num"] = message["segment-num"]
        self._handoff_message["action"] = "archive"
        self._handoff_message["server-node-names"] = [
            client.server_node_name for client in self._resilient_clients
        ]

        return True # completed

    def _handle_archive_key_start(self, message):
        assert len(self._handoff_message) == 0, self._handoff_message
        self._handoff_message["message-type"] = "hinted-handoff"
        self._handoff_message["dest-node-name"] = self._original_dest_node_name
        self._handoff_message["avatar-id"] = message["avatar-id"]
        self._handoff_message["timestamp-repr"] = message["timestamp-repr"]
        self._handoff_message["key"] = message["key"]
        self._handoff_message["segment-num"] = message["segment-num"]
        self._handoff_message["action"] = "archive"
        self._handoff_message["server-node-names"] = [
            client.server_node_name for client in self._resilient_clients
        ]

        return False # not completed

    def _handle_archive_key_next(self, message):
        assert self._handoff_message["message-type"] == "hinted-handoff", \
                self._handoff_message
        assert self._handoff_message["avatar-id"] == message["avatar-id"]
        assert self._handoff_message["key"] == message["key"]

        return False # not completed
    
    def _handle_archive_key_final(self, message):
        assert self._handoff_message["message-type"] == "hinted-handoff", \
                self._handoff_message
        assert self._handoff_message["avatar-id"] == message["avatar-id"]
        assert self._handoff_message["key"] == message["key"]

        return True # completed
    
    def _handle_destroy_key(self, message):
        assert len(self._handoff_message) == 0, self._handoff_message
        self._handoff_message["message-type"] = "hinted-handoff"
        self._handoff_message["dest-node-name"] = self._original_dest_node_name
        self._handoff_message["avatar-id"] = message["avatar-id"]
        self._handoff_message["timestamp-repr"] = message["timestamp-repr"]
        self._handoff_message["key"] = message["key"]
        self._handoff_message["segment-num"] = message["segment-num"]
        self._handoff_message["action"] = "destroy"
        self._handoff_message["server-node-names"] = [
            client.server_node_name for client in self._resilient_clients
        ]

        return True # completed

