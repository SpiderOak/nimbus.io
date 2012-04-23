# -*- coding: utf-8 -*-
"""
data_reader_main.py

Receives block read requests.
Looks up pointers to data by querying the database server
Looks for files in both the hashfanout area 
Responds with content or "not available"
"""
from base64 import b64encode
from collections import deque, namedtuple
import hashlib
import logging
import os.path
import sys
import time
import zlib

import zmq

import Statgrabber

from tools.data_definitions import encoded_block_generator
from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.rep_server import REPServer
from tools.event_push_client import EventPushClient, exception_event
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process
from tools.database_connection import get_node_local_connection

from data_reader.reader import Reader
from data_reader.state_cleaner import StateCleaner
from data_reader.stats_reporter import StatsReporter

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_data_reader_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name
)
_data_reader_address = os.environ["NIMBUSIO_DATA_READER_ADDRESS"]
_data_reader_anti_entropy_address = \
        os.environ["NIMBUSIO_DATA_READER_ANTI_ENTROPY_ADDRESS"]
_retrieve_timeout = 30 * 60.0
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]

_retrieve_state_tuple = namedtuple("RetrieveState", [ 
    "generator",
    "sequence_row_count",
    "sequence_read_count",
    "block_count",
    "blocks_sent",
    "timeout",
])

def _compute_state_key(message):
    """
    compute a key to the state for this message
    """
    return (message["client-tag"],
            message["segment-unified-id"], 
            message["segment-conjoined-part"], 
            message["segment-num"], )

def _str_state_key(state_key):
    return "%s %s conjoined-part=%s segment-num=%s" % state_key

def _handle_retrieve_key_start(state, message, _data):
    log = logging.getLogger("_handle_retrieve_key_start")
    state_key = _compute_state_key(message)
    log.info("{0} block_offset={1}, block_count={2}".format(
        _str_state_key(state_key),
        message["block-offset"],
        message["block-count"]))

    reply = {
        "message-type"          : "retrieve-key-reply",
        "client-tag"            : message["client-tag"],
        "message-id"            : message["message-id"],
        "segment-unified-id"    : message["segment-unified-id"],
        "segment-conjoined-part": message["segment-conjoined-part"],
        "segment-num"           : message["segment-num"],
        "segment-size"          : None,
        "zfec-padding-size"     : None,
        "segment-adler32"       : None,
        "segment-md5-digest"    : None,
        "sequence-num"          : None,
        "completed"             : None,
        "result"                : None,
        "error-message"         : None,
    }

    # if we already have a state entry for this request, something is wrong
    if state_key in state["active-requests"]:
        error_string = "invalid duplicate request in retrieve-key-start"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    sequence_generator = state["reader"].generate_all_sequence_rows(
        message["segment-unified-id"],
        message["segment-conjoined-part"],
        message["segment-num"],
        message["handoff-node-id"],
        message["block-offset"]
    )

    sequence_row_count, sequence_rows_skipped, offset_residue = \
            sequence_generator.next()

    if sequence_row_count == 0:
        error_string = "no sequence rows found"
        log.error(error_string)
        reply["result"] = "no-sequence-rows"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    log.debug("found={0} skipped={1} offset_residue={2}".format(
        sequence_row_count, sequence_rows_skipped, offset_residue
    ))

    try:
        sequence_row, segment_data = sequence_generator.next()
    except Exception, instance:
        log.exception("retrieving")
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    segment_md5 = hashlib.md5(segment_data)
    if segment_md5.digest() != str(sequence_row.hash):
        error_message = "md5 mismatch %s" % (_str_state_key(state_key), )
        log.error(error_message)
        state["event-push-client"].error("md5-mismatch", error_message)  
        reply["result"] = "md5-mismatch"
        reply["error-message"] = "segment md5 does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    encoded_block_list = list(encoded_block_generator(segment_data))

    recompute = False

    if offset_residue > 0:
        encoded_block_list = encoded_block_list[offset_residue:]
        recompute = True

    if message["block-count"] is not None:
        if len(encoded_block_list) > message["block-count"]:
            encoded_block_list = encoded_block_list[:message["block-count"]]
            recompute = True

    assert len(encoded_block_list) > 0

    Statgrabber.accumulate('nimbusio_read_requests', 1)
    Statgrabber.accumulate('nimbusio_read_bytes', sequence_row.size)

    state_entry = _retrieve_state_tuple(
        generator=sequence_generator,
        sequence_row_count=sequence_row_count,
        sequence_read_count=1,
        block_count=message["block-count"],
        blocks_sent=len(encoded_block_list), 
        timeout=time.time() + _retrieve_timeout
    )

    # save stuff we need to recall in state
    if state_entry.sequence_read_count == state_entry.sequence_row_count \
    or (state_entry.block_count is not None 
        and state_entry.blocks_sent == state_entry.block_count):
        reply["completed"] = True
    else:
        reply["completed"] = False
        state["active-requests"][state_key] = state_entry

    segment_size = sequence_row.size
    segment_adler32 = sequence_row.adler32
    segment_md5_digest = sequence_row.hash

    # if we chopped some blocks out of the data, we must recompute
    # the check values
    if recompute:
        segment_size = 0
        segment_adler32 = 0
        segment_md5 = hashlib.md5()
        for encoded_block in encoded_block_list:
            segment_size += len(encoded_block)
            segment_adler32 = zlib.adler32(encoded_block, segment_adler32) 
            segment_md5.update(encoded_block)
        segment_md5_digest = segment_md5.digest()

    reply["sequence-num"] = state_entry.sequence_read_count
    reply["segment-size"] = segment_size
    reply["zfec-padding-size"] = sequence_row.zfec_padding_size
    reply["segment-adler32"] = segment_adler32
    reply["segment-md5-digest"] = b64encode(segment_md5_digest)
    reply["result"] = "success"
    state["resilient-server"].send_reply(reply, data=encoded_block_list)

def _handle_retrieve_key_next(state, message, _data):
    log = logging.getLogger("_handle_retrieve_key_next")
    state_key = _compute_state_key(message)
    log.info("{0} block_offset={1}, block_count={2}".format(
        _str_state_key(state_key),
        message["block-offset"],
        message["block-count"]))

    reply = {
        "message-type"          : "retrieve-key-reply",
        "client-tag"            : message["client-tag"],
        "message-id"            : message["message-id"],
        "segment-unified-id"    : message["segment-unified-id"],
        "segment-num"           : message["segment-num"],
        "segment-size"          : None,
        "zfec-padding-size"     : None,
        "segment-adler32"       : None,
        "segment-md5-digest"    : None,
        "sequence-num"          : None,
        "completed"             : None,
        "result"                : None,
        "error-message"         : None,
    }

    try:
        state_entry = state["active-requests"].pop(state_key)
    except KeyError:
        error_string = "unknown request %r" % (_str_state_key(state_key), )
        log.error(error_string)
        reply["result"] = "unknown-request"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    try:
        sequence_row, segment_data = state_entry.generator.next()
    except Exception, instance:
        log.exception("retrieving")
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    segment_md5 = hashlib.md5(segment_data)
    if segment_md5.digest() != str(sequence_row.hash):
        error_message = "md5 mismatch %s" % (_str_state_key(state_key), )
        log.error(error_message)
        state["event-push-client"].error("md5-mismatch", error_message)  
        reply["result"] = "md5-mismatch"
        reply["error-message"] = "segment md5 does not match expected value"
        state["resilient-server"].send_reply(reply)
        return

    encoded_block_list = list(encoded_block_generator(segment_data))
    blocks_sent = state_entry.blocks_sent + len(encoded_block_list)

    recompute = False
    if state_entry.block_count is not None and \
       blocks_sent > state_entry.block_count:
        block_delta = blocks_sent = state_entry.block_count
        encoded_block_list = encoded_block_list[:-block_delta]
        blocks_sent = state_entry.block_count
        recompute = True

    Statgrabber.accumulate('nimbusio_read_requests', 1)
    Statgrabber.accumulate('nimbusio_read_bytes', sequence_row.size)

    sequence_read_count = state_entry.sequence_read_count + 1

    if sequence_read_count == state_entry.sequence_row_count or \
       (state_entry.block_count is not None and \
        state_entry.blocks_sent == state_entry.block_count):
        reply["completed"] = True
    else:
        reply["completed"] = False
        state["active-requests"][state_key] = state_entry._replace(
            sequence_read_count=sequence_read_count,
            blocks_sent=blocks_sent
        )

    segment_size = sequence_row.size
    segment_adler32 = sequence_row.adler32
    segment_md5_digest = sequence_row.hash

    # if we chopped some blocks out of the data, we must recompute
    # the check values
    if recompute:
        segment_size = 0
        segment_adler32 = 0
        segment_md5 = hashlib.md5()
        for encoded_block in encoded_block_list:
            segment_size += len(encoded_block)
            segment_adler32 = zlib.adler32(encoded_block, segment_adler32) 
            segment_md5.update(encoded_block)
        segment_md5_digest = segment_md5.digest()


    reply["sequence-num"] = sequence_read_count
    reply["segment-size"] = segment_size
    reply["zfec-padding-size"] = sequence_row.zfec_padding_size
    reply["segment-adler32"] = segment_adler32
    reply["segment-md5-digest"] = b64encode(segment_md5_digest)
    reply["result"] = "success"
    state["resilient-server"].send_reply(reply, data=encoded_block_list)

def _handle_web_server_start(state, message, _data):
    log = logging.getLogger("_handle_web_server_start")
    log.info("%s %s %s" % (message["unified-id"], 
                           message["timestamp-repr"],
                           message["source-node-name"]))

_dispatch_table = {
    "retrieve-key-start"    : _handle_retrieve_key_start,
    "retrieve-key-next"     : _handle_retrieve_key_next,
    "web-server-start"      : _handle_web_server_start,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "resilient-server"      : None,
        "anti-entropy-server"   : None,
        "event-push-client"     : None,
        "stats-reporter"        : None,
        "state-cleaner"         : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
        "active-requests"       : dict(),
        "database-connection"   : None,
        "reader"                : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "data_reader"
    )

    log.info("binding resilient-server to {0}".format(_data_reader_address))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _data_reader_address,
        state["receive-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    log.info("binding anti-entropy-server to {0}".format(
        _data_reader_anti_entropy_address))
    state["anti-entropy-server"] = REPServer(
        state["zmq-context"],
        _data_reader_anti_entropy_address,
        state["receive-queue"]
    )
    state["anti-entropy-server"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["state-cleaner"] = StateCleaner(state)

    state["database-connection"] = get_node_local_connection()

    state["reader"] = Reader(
        state["database-connection"],
        _repository_path
    )

    state["stats-reporter"] = StatsReporter(state)

    state["event-push-client"].info("program-start", "data_reader starts")  

    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["state-cleaner"].run, state["state-cleaner"].next_run(), ), 
        (state["stats-reporter"].run, state["stats-reporter"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping resilient server")
    state["resilient-server"].close()
    state["anti-entropy-server"].close()
    state["event-push-client"].close()

    state["zmq-context"].term()

    state["reader"].close()
    state["database-connection"].close()

    log.debug("teardown complete")

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ],
            exception_action=exception_event
        )
    )

