# -*- coding: utf-8 -*-
"""
io_worker.py

One of a pool of io workers
"""
from base64 import b64encode
from collections import namedtuple
import hashlib
import logging
import os
import os.path
import sys
from threading import Event
import time
import zlib

import zmq

from tools.standard_logging import initialize_logging
from tools.data_definitions import compute_value_file_path, \
        encoded_block_slice_size, \
        encoded_block_generator
from tools.LRUCache import LRUCache
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall
from tools.process_util import set_signal_handler
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from retrieve_source.internal_sockets import io_controller_router_socket_uri

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_io_worker_{1}_{2}_{3}.log"
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_max_file_cache_size = 1000
_unused_file_close_interval = 120.0

_resources_tuple = namedtuple("Resources", 
                              ["halt_event",
                               "zeromq_context",
                               "reply_push_sockets",
                               "dealer_socket",
                               "event_push_client",
                               "file_cache", ])

def _send_work_request(resources, volume_name):
    """
    start the work cycle by notifying the controller that we are available
    """
    log = logging.getLogger("_send_initial_work_request")
    log.debug("sending initial request")
    message = {"message-type" : "ready-for-work",
               "volume-name"  : volume_name,}
    resources.dealer_socket.send_pyobj(message)

def _get_reply_push_socket(resources, client_pull_address):
    log = logging.getLogger("_get_reply_push_socket")
    if not client_pull_address in resources.reply_push_sockets:
        push_socket = resources.zeromq_context.socket(zmq.PUSH)
        push_socket.setsockopt(zmq.LINGER, 5000)
        log.info("connecting to {0}".format(client_pull_address))
        push_socket.connect(client_pull_address)
        resources.reply_push_sockets[client_pull_address] = push_socket
    return resources.reply_push_sockets[client_pull_address]

def _send_error_reply(resources, message, control):
    """
    so send the error reply here.
    """
    log = logging.getLogger("_send_error_reply")
    push_socket = _get_reply_push_socket(resources, message["client-address"])

    reply = {"message-type"          : "retrieve-key-reply",
             "user-request-id"       : message["user-request-id"],
             "client-tag"            : message["client-tag"],
             "message-id"            : message["message-id"],
             "retrieve-id"           : message["retrieve-id"],
             "segment-unified-id"    : message["segment-unified-id"],
             "segment-num"           : message["segment-num"],
             "result"                : control["result"],
              "error-message"        : control["error-message"],}
    push_socket.send_json(reply)

def _process_request(resources):
    """
    Wait for a reply to our last message from the controller.
    """
    log = logging.getLogger("_process_one_transaction")
    log.debug("waiting work request")
    try:
        request = resources.dealer_socket.recv_pyobj()
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error):
            raise InterruptedSystemCall()
        raise

    assert resources.dealer_socket.rcvmore
    control = resources.dealer_socket.recv_pyobj()

    log.debug("user_request_id = {0}; control = {1}".format(
              request["user-request-id"], control))

    assert resources.dealer_socket.rcvmore
    sequence_row = resources.dealer_socket.recv_pyobj()

    value_file_path = compute_value_file_path(_repository_path, 
                                              sequence_row["space_id"], 
                                              sequence_row["value_file_id"]) 

    control["result"] = "success"
    control["error-message"] = ""

    if value_file_path in resources.file_cache:
        value_file, _ = resources.file_cache[value_file_path]
        del resources.file_cache[value_file_path]
    else:
        try:
            value_file = open(value_file_path, "rb")
        except Exception as instance:
            log.exception("user_request_id = {0}, " \
                          "read {1}".format(request["user-request-id"],
                                            value_file_path))
            resources.event_push_client.exception("error_opening_value_file", 
                                                  str(instance))
            control["result"] = "error_opening_value_file"
            control["error-message"] = str(instance)
            
        if control["result"] != "success":
            _send_error_reply(resources, request, control)
            return

    read_offset = \
        sequence_row["value_file_offset"] + \
        (control["left-offset"] * encoded_block_slice_size)

    read_size = \
        sequence_row["size"] - \
        (control["left-offset"] * encoded_block_slice_size) - \
        (control["right-offset"] * encoded_block_slice_size)

    try:
        value_file.seek(read_offset)
        encoded_data = value_file.read(read_size)
    except Exception as instance:
        log.exception("user_request_id = {0}, " \
                      "read {1}".format(request["user-request-id"],
                                        value_file_path))
        resources.event_push_client.exception("error_reading_value_file", 
                                              str(instance))
        control["result"] = "error_reading_vqalue_file"
        control["error-message"] = str(instance)

    if control["result"] != "success":
        value_file.close()
        _send_error_reply(resources, request, control)
        return

    resources.file_cache[value_file_path] = value_file, time.time()

    if len(encoded_data) != read_size:
        error_message = "{0} size mismatch {1} {2}".format(
            request["retrieve-id"],
            len(encoded_data),
            read_size)
        log.error("user_request_id = {0}, {1}".format(request["user-request-id"],
                                                      error_message))
        resources.event_push_client.error("size_mismatch", error_message)
        control["result"] = "size_mismatch"
        control["error-message"] = error_message

    if control["result"] != "success":
        _send_error_reply(resources, request, control)
        return

    encoded_block_list = list(encoded_block_generator(encoded_data))

    segment_size = 0
    segment_adler32 = 0
    segment_md5 = hashlib.md5()
    for encoded_block in encoded_block_list:
        segment_size += len(encoded_block)
        segment_adler32 = zlib.adler32(encoded_block, segment_adler32) 
        segment_md5.update(encoded_block)
    segment_md5_digest = segment_md5.digest()

    reply = {
        "message-type"          : "retrieve-key-reply",
        "user-request-id"       : request["user-request-id"],
        "client-tag"            : request["client-tag"],
        "message-id"            : request["message-id"],
        "retrieve-id"           : request["retrieve-id"],
        "segment-unified-id"    : request["segment-unified-id"],
        "segment-num"           : request["segment-num"],
        "segment-size"          : segment_size,
        "zfec-padding-size"     : sequence_row["zfec_padding_size"],
        "segment-adler32"       : segment_adler32,
        "segment-md5-digest"    : b64encode(segment_md5_digest).decode("utf-8"),
        "sequence-num"          : None,
        "completed"             : control["completed"],
        "result"                : "success",
        "error-message"         : "",
    }

    push_socket = _get_reply_push_socket(resources, request["client-address"])
    push_socket.send_json(reply, zmq.SNDMORE)
    for encoded_block in encoded_block_list[:-1]:
        push_socket.send(encoded_block, zmq.SNDMORE)
    push_socket.send(encoded_block_list[-1])
        
def _make_close_pass(resources, current_time):
    log = logging.getLogger("_make_close_pass")

    files_to_close = list()
    for file_name, (_, last_used_time) in resources.file_cache.iteritems():
        unused_interval = current_time - last_used_time
        if unused_interval > _unused_file_close_interval:
            files_to_close.append(file_name)

    log.debug("{0} files to close".format(len(files_to_close)))
    for file_name in files_to_close:
        log.info("closing {0}".format(file_name))
        file_object, _ = resources.file_cache[file_name]
        del resources.file_cache[file_name]
        file_object.close()

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0

    volume_name = sys.argv[1]
    worker_number = int(sys.argv[2])

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         volume_name.replace("/", "_"),
                                         worker_number,
                                         _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    event_source_name = "rs_io_worker_{0}_{1}".format(volume_name, 
                                                      worker_number)
    resources = \
        _resources_tuple(halt_event=halt_event,
                         zeromq_context=zeromq_context,
                         reply_push_sockets=dict(),
                         event_push_client=EventPushClient(zeromq_context, 
                                                           event_source_name),
                         dealer_socket=zeromq_context.socket(zmq.DEALER),
                         file_cache=LRUCache(_max_file_cache_size))

    resources.dealer_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting to {0}".format(io_controller_router_socket_uri))
    resources.dealer_socket.connect(io_controller_router_socket_uri)

    last_close_pass_time = time.time()
    try:
        while not halt_event.is_set():
            # an occasional pass that closes any open files that haven't 
            # been used
            current_time = time.time()
            elapsed_time = current_time - last_close_pass_time
            if elapsed_time > _unused_file_close_interval:
                _make_close_pass(resources, current_time)
                last_close_pass_time = current_time 

            _send_work_request(resources, volume_name)
            _process_request(resources)

    except InterruptedSystemCall:
        if halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("zeromq error processing request")
            resources.event_push_client.exception(
                unhandled_exception_topic,
                "Interrupted zeromq system call",
                exctype="InterruptedSystemCall")
            return_value = 1
    except Exception as instance:
        log.exception("error processing request")
        resources.event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__)
        return_value = 1
    else:
        log.info("program teminates normally")
    finally:
        resources.dealer_socket.close()
        for push_socket in resources.reply_push_sockets.values():
            push_socket.close()
        resources.event_push_client.close()
        resources.zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())
