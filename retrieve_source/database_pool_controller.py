# -*- coding: utf-8 -*-
"""
database_pool_controller.py

Manage a pool of database workers
"""
from collections import deque, namedtuple
import logging
import os
import os.path
import subprocess
import sys
from threading import Event
import time

import zmq

from tools.standard_logging import initialize_logging
from tools.process_util import identify_program_dir, \
        set_signal_handler, \
        poll_subprocess, \
        terminate_subprocess
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.data_definitions import encoded_block_slice_size
from tools.zeromq_util import PollError, \
        is_interrupted_system_call

from retrieve_source.internal_sockets import db_controller_pull_socket_uri, \
        db_controller_router_socket_uri, \
        io_controller_pull_socket_uri

_resources_tuple = namedtuple("Resources", 
                              ["halt_event",
                               "zeromq_context",
                               "reply_push_sockets",
                               "pull_socket",
                               "io_controller_push_socket",
                               "router_socket",
                               "event_push_client",
                               "active_retrieves",
                               "pending_work_queue",
                               "available_ident_queue",])

_retrieve_state_tuple = namedtuple("RetrieveState", 
                                   ["sequence_rows",
                                    "sequence_index",
                                    "left_offset",
                                    "right_offset",
                                    "timestamp", ])

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_db_pool_controller_{1}.log"
_worker_count = int(os.environ.get("NIMBUSIO_RETRIEVE_DB_POOL_COUNT", "2"))
_poll_timeout = 3.0 

def _launch_database_pool_worker(worker_number):
    log = logging.getLogger("launch_database_pool_worker")
    module_dir = identify_program_dir("retrieve_source")
    module_path = os.path.join(module_dir, "database_pool_worker.py")
    
    args = [sys.executable, module_path, str(worker_number), ]

    log.info("starting {0}".format(args))
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _send_pending_work_to_available_workers(resources):
    """
    send messages from the pending_work_queue 
    to workers in the available_ident_queue
    """
    log = logging.getLogger("_send_pending_work_to_available_workers")
    work_count = min(len(resources.pending_work_queue), 
                     len(resources.available_ident_queue))
    for _ in range(work_count):
        message, control = resources.pending_work_queue.popleft()
        ident = resources.available_ident_queue.popleft()
        resources.router_socket.send(ident, zmq.SNDMORE)
        resources.router_socket.send_pyobj(message, zmq.SNDMORE)
        resources.router_socket.send_pyobj(control)

def _handle_retrieve_key_start(resources, message, control):
    log = logging.getLogger("_handle_retrieve_key_start")
    retrieve_id = message["retrieve-id"]
    if retrieve_id in resources.active_retrieves:
        log.error("duplicate retrieve-id {0} in archive-key-start".format(
             retrieve_id))
        del resources.active_retrieves[retrieve_id]

    log.debug("adding {0} to pending work queue".format(message))
    resources.pending_work_queue.append((message, control, ))

def _handle_retrieve_key_next(resources, message, control):
    log = logging.getLogger("_handle_retrieve_key_next")
    retrieve_id = message["retrieve-id"]

    if retrieve_id not in resources.active_retrieves:
        log.error("unknown retrieve-id {0} in archive-key-next".format(
             retrieve_id))
        return

    retrieve_state = resources.active_retrieves.pop(retrieve_id)
    _send_request_to_io_controller(resources, message, control, retrieve_state)

_dispatch_table = { "retrieve-key-start" : _handle_retrieve_key_start,
                    "retrieve-key-next"  : _handle_retrieve_key_next, }

def _read_pull_socket(resources):
    """
    read messages from the PULL socket until we would block
    if the message-type is 'retrieve-key-start'
        add to the pending_work queue

    if the message-type is 'retrieve-key-next'
        get the next segment_slice info from active_retrieves
        add the information to the request
        push the request to the disk_io_controller 
    """
    log = logging.getLogger("_read_pull_socket")

    while True: # read until we would block
        try:
            message = resources.pull_socket.recv_pyobj(zmq.NOBLOCK)
        except zmq.ZMQError as instance:
            if instance.errno == zmq.EAGAIN:
                break
            raise

        assert resources.pull_socket.rcvmore
        control = resources.pull_socket.recv_pyobj()

        try:
            _dispatch_table[message["message-type"]](resources, 
                                                     message, 
                                                     control)
        except KeyError as instance:
            log.error("unknown message type {0} {1}".format(instance, message))

    _send_pending_work_to_available_workers(resources)

def _compute_blocks_in_sequence(sequence_data_size):
    """
    compute the number of encoded blocks in sequence_row.size
    """
    blocks_in_sequence = sequence_data_size // encoded_block_slice_size
    if sequence_data_size % encoded_block_slice_size:
        blocks_in_sequence += 1
    return blocks_in_sequence

def _analyze_slice_offsets(sequence_rows, block_offset, total_block_count):
    """
    if the caller has requested a slice, we may need to skip some sequence rows
    return skip_count     = number of sequence rows to skip
           left_offset    = number of blocks to skip from first remaining
                            sequence row
           right_offset   = number of blocks to skip from the end of
                            the last sequence row
    """
    log = logging.getLogger("_analyze_slice_offsets")
    pre_block_count = 0
    skip_count = 0
    left_offset = 0
    right_offset = 0

    index = 0
    while pre_block_count < block_offset:
        sequence_row = sequence_rows[index]
        blocks_in_sequence = _compute_blocks_in_sequence(sequence_row["size"])
        pre_block_count += blocks_in_sequence
        log.debug("{0} blocks_in_sequence={1}, pre_block_count={2}".format(
            index, blocks_in_sequence, pre_block_count))

        index += 1

    skip_count = index

    if block_offset > 0: 
        if skip_count == 0:
            left_offset = block_offset
        else:
            left_offset = pre_block_count - block_offset
      
    # index points to the first segment we are actually going to send
    if total_block_count is not None:
        blocks_to_send = (0 if left_offset == 0 else -left_offset)
        while blocks_to_send < total_block_count:
            sequence_row = sequence_rows[index]
            blocks_in_sequence = \
                _compute_blocks_in_sequence(sequence_row["size"])
            blocks_to_send += blocks_in_sequence
            log.debug("{0} blocks_in_sequence={1}, blocks_to_send={2}".format(
                index, blocks_in_sequence, blocks_to_send))

            index += 1

        if blocks_to_send > total_block_count:
            right_offset = blocks_to_send - total_block_count

    log.debug("skip_count={0}, left_offset={1}, right_offset={2}".format(
        skip_count, left_offset, right_offset))
    return (skip_count, left_offset, right_offset)

def _send_error_reply(resources, message, control):
    """
    if we failed to get sequence data, there's no point in going on
    so send the error reply here.
    """
    log = logging.getLogger("_send_error_reply")
    if not control["client-pull-address"] in resources.reply_push_sockets:
        push_socket = resources.zeromq_context.socket(zmq.PUSH)
        push_socket.setsockopt(zmq.LINGER, 5000)
        log.info("connecting to {0}".format(control["client-pull-address"]))
        push_socket.connect(control["client-pull-address"])
        resources.reply_push_sockets[control["client-pull-address"]] = \
            push_socket
    push_socket = resources.reply_push_sockets[control["client-pull-address"]]
    reply = {"message-type"          : "archive-key-reply",
             "client-tag"            : message["client-tag"],
             "message-id"            : message["message-id"],
             "retrieve-id"           : message["retrieve-id"],
             "segment-unified-id"    : message["segment-unified-id"],
             "segment-num"           : message["segment-num"],
             "result"                : control["result"],
              "error-message"        : control["error-message"],}
    push_socket.send(reply)

def _read_router_socket(resources):
    """
    read a message from the router socket (from one of our worker processes)
    if the message-type is 'ready-for-work' (initial message)
        add the message ident to the resources.available_ident_queue
    if the message-type is 'archive-key-start'
        use the attached data to start an active_retrieve
        add the message ident to the resources.available_ident_queue
    """
    log = logging.getLogger("_read_router_socket")

    ident = resources.router_socket.recv()
    assert resources.router_socket.rcvmore
    message = resources.router_socket.recv_pyobj()

    if message["message-type"] == "ready-for-work":
        return

    assert resources.router_socket.rcvmore
    control = resources.router_socket.recv_pyobj()

    if control["result"] != "success":
        log.error("{0} {1} {2}".format(message["retrieve-id"],
                                       control["result"],
                                       control["error-message"]))
        _send_error_reply(resources, message, control)
        return

    assert resources.router_socket.rcvmore
    sequence_rows = resources.router_socket.recv_pyobj()

    resources.available_ident_queue.append(ident) 
    _send_pending_work_to_available_workers(resources)

    assert  message["message-type"] == "retrieve-key-start", message
    assert sequence_rows is not None

    row_skip_count, left_offset, right_offset = \
        _analyze_slice_offsets(sequence_rows, 
                               message["block-offset"],
                               message["block-count"])
    log.debug(
        "{0} {1} rows; skip={2}, left_offset={3}, right_offset={4} ".format(
            message["retrieve-id"],
            len(sequence_rows),
            row_skip_count,
            left_offset,
            right_offset))

    retrieve_state = _retrieve_state_tuple(sequence_rows=sequence_rows,
                                           sequence_index=row_skip_count,
                                           left_offset=left_offset,
                                           right_offset=right_offset,
                                           timestamp=time.time())

    _send_request_to_io_controller(resources, message, control, retrieve_state)

def _send_request_to_io_controller(resources, 
                                   message, 
                                   control, 
                                   retrieve_state):
    log = logging.getLogger("_send_request_to_io_controller")
    log.debug("{0} sending row[{1}] of {2}".format(
        message["retrieve-id"],
        retrieve_state.sequence_index,
        len(retrieve_state.sequence_rows)))

    if message["message-type"] == "retrieve-key-start":
        control["left-offset"] = retrieve_state.left_offset
    else:
        control["left-offset"] = 0

    sequence_row = retrieve_state.sequence_rows[retrieve_state.sequence_index]

    next_sequence_index = retrieve_state.sequence_index +1
    assert next_sequence_index <= len(retrieve_state.sequence_rows)
    control["completed"] = \
        next_sequence_index == len(retrieve_state.sequence_rows)

    if control["completed"]:
        control["right-offset"] = retrieve_state.right_offset
    else:
        control["right-offset"] = 0
        resources.active_retrieves[message["retrieve-id"]] = \
            retrieve_state._replace(sequence_index=next_sequence_index)

    resources.io_controller_push_socket.send_pyobj(message, zmq.SNDMORE)
    resources.io_controller_push_socket.send_pyobj(control, zmq.SNDMORE)
    resources.io_controller_push_socket.send_pyobj(sequence_row)

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    resources = \
        _resources_tuple(halt_event=Event(),
                         zeromq_context=zeromq_context,
                         reply_push_sockets=dict(),
                         pull_socket=zeromq_context.socket(zmq.PULL),
                         io_controller_push_socket=\
                            zeromq_context.socket(zmq.PUSH),
                         router_socket=zeromq_context.socket(zmq.ROUTER),
                         event_push_client=\
                            EventPushClient(zeromq_context, 
                                            "rs_db_pool_controller"),
                         active_retrieves=dict(),
                         pending_work_queue=deque(),
                         available_ident_queue=deque())

    log.debug("binding to {0}".format(db_controller_pull_socket_uri))
    resources.pull_socket.bind(db_controller_pull_socket_uri)

    log.debug("connecting to {0}".format(io_controller_pull_socket_uri))
    resources.io_controller_push_socket.connect(io_controller_pull_socket_uri)

    resources.router_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("binding to {0}".format(db_controller_router_socket_uri))
    resources.router_socket.bind(db_controller_router_socket_uri)

    # we poll the sockets for readability, we assume we can always
    # write to the router socket
    poller = zmq.Poller()
    poller.register(resources.pull_socket, zmq.POLLIN | zmq.POLLERR)
    poller.register(resources.router_socket, zmq.POLLIN| zmq.POLLERR)

    worker_processes = list()
    for index in range(_worker_count):
        worker_processes.append(_launch_database_pool_worker(index+1))
    
    try:
        while not halt_event.is_set():
            for worker_process in worker_processes:
                poll_subprocess(worker_process)
            for active_socket, event_flags in poller.poll(_poll_timeout):
                if event_flags & zmq.POLLERR:
                    error_message = \
                        "error flags from zmq {0}".format(active_socket)
                    log.error(error_message)
                    raise PollError(error_message) 
                if active_socket is resources.pull_socket:
                    _read_pull_socket(resources)
                elif active_socket is resources.router_socket:
                    _read_router_socket(resources)
                else:
                    log.error("unknown socket {0}".format(active_socket))
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error) and halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("zeromq error processing request")
            resources.event_push_client.exception(unhandled_exception_topic,
                                                  "zeromq_error",
                                                  exctype="ZMQError")
            return_value = 1
    except Exception as instance:
        log.exception("error processing request")
        resources.event_push_client.exception(unhandled_exception_topic,
                                    str(instance),
                                    exctype=instance.__class__.__name__)
        return_value = 1
    else:
        log.info("program teminates normally")
    finally:
        for worker_process in worker_processes:
            terminate_subprocess(worker_process)
        resources.pull_socket.close()
        resources.io_controller_push_socket.close()
        resources.router_socket.close()
        for push_socket in resources.reply_push_sockets.values():
            push_socket.close()
        resources.event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

