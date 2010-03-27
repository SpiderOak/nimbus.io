# -*- coding: utf-8 -*-
"""
test_upload.py

test uploading a file to the diyapi cluster
"""
import hashlib
from collections import namedtuple
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
_archive_key_start_reply_routing_key = "test_upload.archive_key_start_reply"
_archive_key_next_reply_routing_key = "test_upload.archive_key_next_reply"
_archive_key_final_reply_routing_key = "test_upload.archive_key_final_reply"
_retrieve_key_start_reply_routing_key = "test_upload.retrieve_key_start_reply"
_retrieve_key_next_reply_routing_key = "test_upload.retrieve_key_next_reply"
_retrieve_key_final_reply_routing_key = "test_upload.retrieve_key_final_reply"
_avatar_id = 1001
_segment_size = 120 * 1024


def _pre_loop_function(state):
    log = logging.getLogger("_pre_loop_function")
    input_path = sys.argv[1]
    input_size = os.path.getsize(input_path)
    state["segment-count"] = input_size / _segment_size
    if input_size % _segment_size != 0:
        state["segment-count"] += 1

    log.info("input path = %s %s segments" % (
        input_path, state["segment-count"]
    ))

    state["input-file"] = open(input_path, "r")
    state["key"] = os.path.basename(input_path)
    state["sequence"] = 0
    state["md5"] = hashlib.md5()

    segment = state["input-file"].read(_segment_size)
    state["md5"].update(segment)

    request_id = uuid.uuid1().hex
    local_exchange = amqp_connection.local_exchange_name
    message = ArchiveKeyStart(
        request_id,
        _avatar_id,
        local_exchange,
        _archive_key_start_reply_routing_key,
        state["key"], 
        time.time(),
        state["sequence"],
        42,
        _segment_size,
        segment
    )
    marshalled_message = message.marshall()
    return [(local_exchange, ArchiveKeyStart.routing_key, message), ]

def _handle_archive_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_start_reply")
    message = ArchiveKeyStartReply.unmarshall(message_body)

def _handle_archive_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_next_reply")
    message = ArchiveKeyNextReply.unmarshall(message_body)

def _handle_archive_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_final_reply")
    message = ArchiveKeyFinalReply.unmarshall(message_body)

def _handle_retrieve_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_start_reply")
    message = RetrieveKeyStartReply.unmarshall(message_body)

def _handle_retrieve_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_next_reply")
    message = RetrieveKeyNextReply.unmarshall(message_body)

def _handle_retrieve_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_final_reply")
    message = RetrieveKeyFinalReply.unmarshall(message_body)


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

