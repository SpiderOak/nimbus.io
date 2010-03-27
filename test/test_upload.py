# -*- coding: utf-8 -*-
"""
test_upload.py

test uploading a file to the diyapi cluster
"""
import hashlib
import logging
import os.path
import sys
import time
import uuid

from tools import amqp_connection
from tools import message_driven_process as process

from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply

_log_path = u"/var/log/pandora/test_upload.log"

_queue_name = "test_upload"
_routing_key_binding = "test_upload.*"
_reply_routing_header = "test_upload"
_archive_key_start_reply_routing_key = "%s.%s" % (
    _reply_routing_header, ArchiveKeyStartReply.routing_tag
)
_archive_key_next_reply_routing_key = "%s.%s" % (
    _reply_routing_header, ArchiveKeyNextReply.routing_tag
)
_archive_key_final_reply_routing_key = "%s.%s" % (
    _reply_routing_header, ArchiveKeyFinalReply.routing_tag
)
_retrieve_key_start_reply_routing_key = "%s.%s" % (
    _reply_routing_header, RetrieveKeyStartReply.routing_tag
)
_retrieve_key_next_reply_routing_key = "%s.%s" % (
    _reply_routing_header, RetrieveKeyNextReply.routing_tag
)
_retrieve_key_final_reply_routing_key = "%s.%s" % (
    _reply_routing_header, RetrieveKeyFinalReply.routing_tag
)
_avatar_id = 1001
_segment_size = 120 * 1024


def _pre_loop_function(state):
    log = logging.getLogger("_pre_loop_function")
    input_path = sys.argv[1]
    state["total-size"] = os.path.getsize(input_path)
    state["segment-count"] = state["total-size"] / _segment_size
    if state["total-size"] % _segment_size != 0:
        state["segment-count"] += 1

    log.info("input path = %s %s segments" % (
        input_path, state["segment-count"]
    ))

    state["input-file"] = open(input_path, "r")
    state["key"] = os.path.basename(input_path)
    state["sequence"] = 0
    state["archive-md5"] = hashlib.md5()

    segment = state["input-file"].read(_segment_size)
    state["archive-md5"].update(segment)

    state["archive-request-id"] = uuid.uuid1().hex
    local_exchange = amqp_connection.local_exchange_name
    message = ArchiveKeyStart(
        state["archive-request-id"],
        _avatar_id,
        local_exchange,
        _reply_routing_header,
        state["key"], 
        time.time(),
        state["sequence"],
        42,
        _segment_size,
        segment
    )
    return [(local_exchange, ArchiveKeyStart.routing_key, message), ]

def _prepare_archive_message(state):
    log = logging.getLogger("_prepare_archive_message")
    segment = state["input-file"].read(_segment_size)
    state["archive-md5"].update(segment)

    state["sequence"] += 1
    log.debug("sequence = %s" % (state["sequence"], ))

    if state["sequence"] < state["segment-count"]-1:
        message = ArchiveKeyNext(
            state["archive-request-id"],
            state["sequence"],
            segment
        )
    else:
        state["input-file"].close()
        del state["input-file"]
        message = ArchiveKeyFinal(
            state["archive-request-id"],
            state["sequence"],
            state["total-size"],
            42,
            state["archive-md5"].hexdigest(),
            segment
        )
    return message

def _handle_archive_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_start_reply")
    message = ArchiveKeyStartReply.unmarshall(message_body)
    log.info("reply result = %s" % (message.result, ))
    assert message.request_id == state["archive-request-id"]
    assert message.result == 0, message.error_message 
    
    message = _prepare_archive_message(state)
    local_exchange = amqp_connection.local_exchange_name
    return [(local_exchange, message.routing_key, message), ]

def _handle_archive_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_next_reply")
    message = ArchiveKeyNextReply.unmarshall(message_body)
    log.info("reply result = %s" % (message.result, ))
    assert message.request_id == state["archive-request-id"]
    assert message.result == 0, message.error_message 
    
    message = _prepare_archive_message(state)
    local_exchange = amqp_connection.local_exchange_name
    return [(local_exchange, message.routing_key, message), ]

def _handle_archive_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_final_reply")
    message = ArchiveKeyFinalReply.unmarshall(message_body)
    log.info("reply result = %s" % (message.result, ))

    assert message.request_id == state["archive-request-id"]
    assert message.result == 0, message.error_message 

    state["sequence"] = 0

    local_exchange = amqp_connection.local_exchange_name
    state["retrieve-request-id"] = uuid.uuid1().hex
    message = RetrieveKeyStart(
        state["retrieve-request-id"],
        _avatar_id,
        local_exchange,
        _reply_routing_header,
        state["key"], 
    )
    return [(local_exchange, message.routing_key, message), ]

def _prepare_retrieve_message(state):
    log = logging.getLogger("_next_retrieve_message")

    state["sequence"] += 1
    log.debug("sequence = %s" % (state["sequence"], ))

    if state["sequence"] < state["segment-count"]-1:
        message = RetrieveKeyNext(
            state["retrieve-request-id"],
            state["sequence"]
        )
    else:
        message = RetrieveKeyFinal(
            state["retrieve-request-id"],
            state["sequence"]
        )
    return message

def _handle_retrieve_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_start_reply")
    message = RetrieveKeyStartReply.unmarshall(message_body)
    log.info("reply result = %s" % (message.result, ))
    assert message.request_id == state["retrieve-request-id"]
    assert message.result == 0, message.error_message 

    state["retrieve-md5"] = hashlib.md5()
    state["retrieve-md5"].update(message.data_content)

    message = _prepare_retrieve_message(state)

    local_exchange = amqp_connection.local_exchange_name
    message = RetrieveKeyNext(state["retrieve-request-id"], state["sequence"])
    return [(local_exchange, message.routing_key, message), ]

def _handle_retrieve_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_next_reply")
    message = RetrieveKeyNextReply.unmarshall(message_body)
    log.info("reply result = %s" % (message.result, ))
    assert message.request_id == state["retrieve-request-id"]
    assert message.result == 0, message.error_message 
    state["retrieve-md5"].update(message.data_content)

    message = _prepare_retrieve_message(state)

    local_exchange = amqp_connection.local_exchange_name
    message = RetrieveKeyNext(state["retrieve-request-id"], state["sequence"])
    return [(local_exchange, message.routing_key, message), ]

def _handle_retrieve_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_final_reply")
    message = RetrieveKeyFinalReply.unmarshall(message_body)
    log.info("reply result = %s" % (message.result, ))
    assert message.request_id == state["retrieve-request-id"]
    assert message.result == 0, message.error_message 

    state["retrieve-md5"].update(message.data_content)
    assert state["retrieve-md5"].hexdigest() == \
            state["archive-md5"].hexdigest()
    log.info("test successful")
    sys.exit(0)

_dispatch_table = {
    _archive_key_start_reply_routing_key    : _handle_archive_key_start_reply,
    _archive_key_next_reply_routing_key     : _handle_archive_key_next_reply,
    _archive_key_final_reply_routing_key    : _handle_archive_key_final_reply,
    _retrieve_key_start_reply_routing_key   : _handle_retrieve_key_start_reply,
    _retrieve_key_next_reply_routing_key    : _handle_retrieve_key_next_reply,
    _retrieve_key_final_reply_routing_key   : _handle_retrieve_key_final_reply,
}

if __name__ == "__main__":
    state = dict()
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_pre_loop_function
        )
    )

