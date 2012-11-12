# -*- coding: utf-8 -*-
"""
process_segment_rows.py

process handoffs of segment rows
"""
import logging
import itertools
import os
import subprocess
import sys
import time

import zmq

from tools.zeromq_util import ipc_socket_uri, prepare_ipc_path
from tools.process_util import identify_program_dir, \
        poll_subprocess, \
        terminate_subprocess

_socket_dir = os.environ["NIMBUSIO_SOCKET_DIR"]
_socket_high_water_mark = 1000
_polling_interval = 1.0

def _start_worker_process(worker_id, work_socket_uri, result_socket_uri):
    module_dir = identify_program_dir("handoff_client")
    module_path = os.path.join(module_dir, "worker.py")
    
    args = [sys.executable, 
            module_path, 
            worker_id, 
            work_socket_uri, 
            result_socket_uri, ]
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _key_function(segment_row):
    return (segment_row["unified_id"], segment_row["conjoined_part"], )

def _generate_segment_rows(raw_segment_rows):
    """
    yield tuples of (source_node_ids, segment_row)
    where source_node_ids is a list of the nodes where the segment can be 
    retrieved
    """
    # sort on (unified_id, conjoined_part) to bring pairs together
    raw_segment_rows.sort(key=_key_function)
    group_object = itertools.groupby(raw_segment_rows, _key_function)

    for (_unified_id, _conjoined_id, ), group in group_object:
        segment_row_list = list(group)
        assert len(segment_row_list) > 0
        assert len(segment_row_list) < 3, str(len(segment_row_list))
        source_node_ids = list()
        for segment_row in segment_row_list:
            source_node_ids.append(segment_row["source_node_id"])
        yield (source_node_ids, segment_row_list[0], )

def process_segment_rows(halt_event, zeromq_context, args, raw_segment_rows):
    """
    process handoffs of segment rows
    """
    log = logging.getLogger("process_segment_row")

    work_socket_uri = ipc_socket_uri(_socket_dir, 
                                     args.node_name,
                                     "handoff_client_work")
    prepare_ipc_path(work_socket_uri)

    result_socket_uri = ipc_socket_uri(_socket_dir, 
                                     args.node_name,
                                     "handoff_client_result")
    prepare_ipc_path(result_socket_uri)

    work_socket = zeromq_context.socket(zmq.PUSH)
    work_socket.setsockopt(zmq.HWM, _socket_high_water_mark)
    log.info("binding work socket to {0}".format(work_socket_uri))
    work_socket.bind(work_socket_uri)

    result_socket = zeromq_context.socket(zmq.PULL)
    result_socket.setsockopt(zmq.HWM, _socket_high_water_mark)
    log.info("binding result socket to {0}".format(result_socket_uri))
    result_socket.bind(result_socket_uri)

    work_generator = _generate_segment_rows(raw_segment_rows)

    log.debug("starting workers")
    workers = list()
    for index in range(args.worker_count):
        worker_id = str(index+1)
        workers.append(_start_worker_process(worker_id,
                                             work_socket_uri, 
                                             result_socket_uri))

    log.debug("loading zeromq buffer")
    # fill the zeromq buffer with requests
    pending_handoff_count = 0
    while pending_handoff_count < _socket_high_water_mark and \
        not halt_event.is_set():
        try:
            work_entry = next(work_generator)
        except StopIteration:
            break
        work_socket.send_pyobj(work_entry)
        pending_handoff_count += 1

    # loop until all handoffs have been accomplished
    log.debug("start recv loop: pending handoff count = {0}".format(
        pending_handoff_count))
    while pending_handoff_count > 0 and not halt_event.is_set():
        try:
            reply = result_socket.recv_pyobj(zmq.NOBLOCK)
        except zmq.ZMQError as instance:
            if instance.errno == zmq.EAGAIN: # would block
                reply = None
            else:
                raise
        
        if reply is None:
            for worker in workers:
                poll_subprocess(worker)
            time.sleep(_polling_interval)
            continue

        pending_handoff_count -= 1
        try:
            work_entry = next(work_generator)
        except StopIteration:
            pass
        else:
            work_socket.send_pyobj(work_entry)
            pending_handoff_count += 1

        if not reply["handoff-successful"]:
            log.error("{0} handoff ({1}, {2}) failed: {3}".format(
                reply["worker-id"],
                reply["unified-id"], 
                reply["conjoined-part"],
                reply["error-message"]))
            continue

        log.info("{0} handoff ({1}, {2}) successful".format(
            reply["worker-id"], reply["unified-id"], reply["conjoined-part"]))
    log.debug("recv loop ends")

    for worker in workers:
        terminate_subprocess(worker)

    work_socket.close()
    result_socket.close()

