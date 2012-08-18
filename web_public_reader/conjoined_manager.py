# -*- coding: utf-8 -*-
"""
conjoined_manager.py

functions for conjoined archive data
"""
import logging
from collections import namedtuple

from  gevent.greenlet import Greenlet
import  gevent.pool

from tools.data_definitions import create_priority
from web_server.exceptions import ConjoinedFailedError


_conjoined_timeout = 60.0 * 5.0
_conjoined_list_entry = namedtuple("ConjoinedListEntry", [
        "unified_id",
        "key",
        "create_timestamp", 
        "abort_timestamp",
        "complete_timestamp",]
)

class MessageGreenlet(Greenlet):
    """
    A greenlet to send one message to one data_writer"
    """
    def __init__(self, data_writer, message):
        Greenlet.__init__(self)
        self._resilient_client = data_writer._resilient_client
        self._message = message

    def _run(self):
        delivery_channel = self._resilient_client.queue_message_for_send(
            self._message, data=None
        )
        reply, _data = delivery_channel.get()
        return reply

def list_conjoined_archives(
    connection, 
    collection_id, 
    max_conjoined=1000, 
    key_marker="", 
    conjoined_identifier_marker=0
):
    """
    return a boolean for truncated and list of _conjoined_list_entry
    """

    # ask for one more than max_conjoined so we can tell if we are truncated
    max_conjoined = int(max_conjoined)
    request_count = max_conjoined + 1
    if len(key_marker) == 0:
        conjoined_identifier_marker = 0
    result = connection.fetch_all_rows("""
        select unified_id, key, create_timestamp, abort_timestamp, 
        complete_timestamp from nimbusio_node.conjoined 
        where collection_id = %s and key > %s and unified_id > %s
        and delete_timestamp is null
        and handoff_node_id is null
        order by unified_id
        limit %s
        """.strip(), [
            collection_id, 
            key_marker, 
            conjoined_identifier_marker, 
            request_count
        ]
    )
    truncated = len(result) == request_count
    conjoined_list = [_conjoined_list_entry._make(x) for x in result]
    
    return truncated, conjoined_list

def list_upload_in_conjoined(connection, conjoined_identifier):
    """
    finish a conjoined archive
    """
    pass

def _send_message_receive_reply(data_writers, message, error_tag):
    log = logging.getLogger(error_tag)
    sender_list = list()
    message["priority"] = create_priority()
    pending_group = gevent.pool.Group()
    for data_writer in data_writers:
        # send a copy of the message, so each one gets a separagte message-id
        sender = MessageGreenlet(data_writer, message.copy()) 
        sender_list.append(sender)
        pending_group.start(sender)

    pending_group.join(timeout=_conjoined_timeout)

    for sender in sender_list:
        if not sender.ready():
            log.error("incomplete")
            raise ConjoinedFailedError("%s incomplete" % (error_tag, ))

        if not sender.successful():
            try:
                sender.get()
            except Exception, instance:
                log.exception("")
                raise ConjoinedFailedError("%s %s" % (error_tag, instance, ))

        reply = sender.get()

        if reply["result"] != "success":
            log.error("%s" % (reply, ))
            raise ConjoinedFailedError("%s %s" % (
                error_tag, reply["error-message"]))

