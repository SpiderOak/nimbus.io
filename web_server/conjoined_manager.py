# -*- coding: utf-8 -*-
"""
conjoined_manager.py

functions for conjoined archive data
"""
import logging
import uuid

from  gevent.greenlet import Greenlet
import  gevent.pool

from tools.data_definitions import create_priority

class ConjoinedError(Exception):
    pass

_conjoined_timeout = 60.0 * 5.0

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
    conjoined_identifier_marker=""
):
    """
    return a dict containing "conjoined_list", "truncated"
    where conjoined_list is a list of dicts containing:

    * conjoined_identifier 
    * key
    * create_timestamp 
    * abort_timestamp
    * complete_timestamp 
    * delete_timestamp
    """
    keys = [
        "conjoined_identifier",
        "key",
        "create_timestamp", 
        "abort_timestamp",
        "complete_timestamp",
        "delete_timestamp",
    ]
    timestamp_keys = set([
        "create_timestamp", 
        "abort_timestamp",
        "complete_timestamp",
        "delete_timestamp",
    ])

    # ask for one more than max_conjoined so we can tell if we are truncated
    max_conjoined = int(max_conjoined)
    request_count = max_conjoined + 1
    if len(key_marker) == 0:
        conjoined_identifier_marker = ""
    result = connection.fetch_all_rows("""
        select identifier, key, create_timestamp, abort_timestamp, 
        complete_timestamp, delete_timestamp from nimbusio_node.conjoined where
        collection_id = %s and key > %s and identifier > %s
        order by create_timestamp
        limit %s
        """.strip(), [
            collection_id, 
            key_marker, 
            conjoined_identifier_marker, 
            request_count
        ]
    )
    truncated = len(result) == request_count
    conjoined_list = list()
    for row in result:
        row_dict = dict()
        for key, value in zip(keys, row):
            if key in timestamp_keys and value is not None:
                row_dict[key] = value.isoformat()
            elif key == "conjoined_identifier":
                conjoined_identifier_uuid = uuid.UUID(bytes=value)
                row_dict["conjoined_identifier_hex"] = \
                        conjoined_identifier_uuid.hex
            else:
                row_dict[key] = value
        conjoined_list.append(row_dict)

    return {"conjoined_list" : conjoined_list, "truncated" : truncated}

def start_conjoined_archive(data_writers, collection_id, key, timestamp):
    """
    start a new conjoined archive
    """
    log = logging.getLogger("start_conjoined_archive")

    conjoined_identifier = uuid.uuid1()

    message = {
        "message-type"              : "start-conjoined-archive",
        "collection-id"             : collection_id,
        "key"                       : key,
        "conjoined-identifier-hex"  : conjoined_identifier.hex,
        "timestamp-repr"            : repr(timestamp)
    }

    error_tag = ",".join([str(collection_id), key, conjoined_identifier.hex, ])
    log.info(error_tag)
    _send_message_receive_reply(data_writers, message, error_tag)

    return {"conjoined_identifier_hex" : conjoined_identifier.hex}

def abort_conjoined_archive(
    data_writers, collection_id, key, conjoined_identifier, timestamp
):
    """
    mark a conjoined archive as aborted
    """
    log = logging.getLogger("abort_conjoined_archive")

    message = {
        "message-type"              : "abort-conjoined-archive",
        "collection-id"             : collection_id,
        "key"                       : key,
        "conjoined-identifier-hex"  : conjoined_identifier.hex,
        "timestamp-repr"            : repr(timestamp)
    }

    error_tag = ",".join([str(collection_id), key, conjoined_identifier.hex, ])
    log.info(error_tag)
    _send_message_receive_reply(data_writers, message, error_tag)

def finish_conjoined_archive(
    data_writers, collection_id, key, conjoined_identifier_hex, timestamp
):
    """
    finish a conjoined archive
    """
    log = logging.getLogger("finish_conjoined_archive")

    message = {
        "message-type"              : "finish-conjoined-archive",
        "collection-id"             : collection_id,
        "key"                       : key,
        "conjoined-identifier-hex"  : conjoined_identifier_hex,
        "timestamp-repr"            : repr(timestamp)
    }

    error_tag = ",".join([str(collection_id), key, conjoined_identifier_hex, ])
    log.info(error_tag)
    _send_message_receive_reply(data_writers, message, error_tag)

def list_upload_in_conjoined(connection, conjoined_identifier_hex):
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
            raise ConjoinedError("%s incomplete" % (error_tag, ))

        if not sender.successful():
            try:
                sender.get()
            except Exception, instance:
                log.exception("")
                raise ConjoinedError("%s %s" % (error_tag, instance, ))

        reply = sender.get()

        if reply["result"] != "success":
            log.error("%s" % (reply, ))
            raise ConjoinedError("%s %s" % (error_tag, reply["error-message"]))

