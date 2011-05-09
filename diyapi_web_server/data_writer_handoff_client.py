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

    

class DataWriterHandoffClient(object):
    """
    This class mimics a single ResilientClient while wrapping two clients
    to perform hinted handoff for a node which is down.
    """
    def __init__(
        self, original_server_address, resilient_clients, handoff_client
    ):
        self._log = logging.getLogger("HandoffClient-%s" % (
            original_server_address,
        ))
        self._log.info("handing of to %s" % (resilient_clients, ))
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
        completion_channel = Queue(maxsize=0)

        gevent.spawn(self._complete_handoff, message, data, completion_channel)

        return completion_channel

    def _complete_handoff(self, message, data, completion_channel):
        data_writer_replies = list()

        # hand off the message, 
        # at this stage we don't care what message it is
        data_writer_channels = [
            client.queue_message_for_send(message, data) \
            for client in self._resilient_clients
        ]

        # get all replies from the actual clients  
        for data_writer_channel in data_writer_channels:
            data_writer_reply, _data = data_writer_channel.get()
            data_writer_replies.append(data_writer_reply)

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
            completed = self._dispatch_table[message.control["message-type"]](
                message
            )
        except KeyError:
            self._log.error("Unknown message type %s" % (message.control, ))
            return

        # if we're not done, notify the sender that we are ok so far
        if not completed:
            reply = {
               "message-type"   : "handoff-success",
               "result"         : "success",
               "error-message"  : None,
            }
            message = _message_format(control=reply, body=None)
            completion_channel.put(message)
            return

        # now the archive (or destroy) is fully backed up,
        # we must send the hint to the handoff server
        handoff_client_channel = self._handoff_client.queue_message_for_send(
            self._handoff_message, body=None
        )        

        # just pass on whatever reply the handoff server gave us
        reply, _data = handoff_client_channel.get()
        completion_channel.put(_message_format(control=reply, body=None))

    def _handle_archive_key_entire(self, message):
        assert len(self._handoff_message) == 0, self._handoff_message
        self._handoff_message["message-type"] = "handoff-archive"
        self._handoff_message["avatar-id"] = message["avatar-id"]
        self._handoff_message["key"] = message["key"]
        self._handoff_message["version-number"] = message["version-number"]
        self._handoff_message["segment-number"] = message["segment-number"]
        self._handoff_message["backup-addresses"] = [
            client.server_address for client in self._resilient_clients
        ]

        return True # completed

    def _handle_archive_key_start(self, message):
        assert len(self._handoff_message) == 0, self._handoff_message
        self._handoff_message["message-type"] = "handoff-archive"
        self._handoff_message["avatar-id"] = message["avatar-id"]
        self._handoff_message["key"] = message["key"]
        self._handoff_message["version-number"] = message["version-number"]
        self._handoff_message["segment-number"] = message["segment-number"]
        self._handoff_message["backup-addresses"] = [
            client.server_address for client in self._resilient_clients
        ]

        return False # not completed

    def _handle_archive_key_next(self, message):
        assert self._handoff_message["message-type"] == "handoff-archive"
        assert self._handoff_message["avatar-id"] == message["avatar-id"]
        assert self._handoff_message["key"] == message["key"]

        return False # not completed
    
    def _handle_archive_key_final(self, message):
        assert self._handoff_message["message-type"] == "handoff-archive"
        assert self._handoff_message["avatar-id"] == message["avatar-id"]
        assert self._handoff_message["key"] == message["key"]

        return True # completed
    
    def _handle_destroy_key(self, message):
        assert len(self._handoff_message) == 0, self._handoff_message
        self._handoff_message["message-type"] = "handoff-destroy"
        self._handoff_message["avatar-id"] = message["avatar-id"]
        self._handoff_message["key"] = message["key"]
        self._handoff_message["version-number"] = message["version-number"]
        self._handoff_message["segment-number"] = message["segment-number"]
        self._handoff_message["backup-addresses"] = [
            client.server_address for client in self._resilient_clients
        ]

        return True # completed

