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

import zmq

from tools.zeromq_util import ipc_socket_uri, \
        prepare_ipc_path, \
        is_interrupted_system_call
from tools.process_util import identify_program_dir, \
        poll_subprocess, \
        terminate_subprocess

_socket_dir = os.environ["NIMBUSIO_SOCKET_DIR"]
_socket_high_water_mark = 1000
_polling_interval = 1.0

def _start_worker_process(worker_id, rep_socket_uri):
    module_dir = identify_program_dir("handoff_client")
    module_path = os.path.join(module_dir, "worker.py")
    
    args = [sys.executable, 
            module_path, 
            worker_id, 
            rep_socket_uri, ]
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
        for _ in range(1000):
            yield (source_node_ids, segment_row_list[0], )

def process_segment_rows(halt_event, zeromq_context, args, raw_segment_rows):
    """
    process handoffs of segment rows
    """
    log = logging.getLogger("process_segment_row")

    log.debug("creating socket")
    rep_socket_uri = ipc_socket_uri(_socket_dir, 
                                    args.node_name,
                                    "handoff_client")
    prepare_ipc_path(rep_socket_uri)

    rep_socket = zeromq_context.socket(zmq.REP)
    rep_socket.setsockopt(zmq.HWM, _socket_high_water_mark)
    log.info("binding rep socket to {0}".format(rep_socket_uri))
    rep_socket.bind(rep_socket_uri)

    log.debug("starting workers")
    workers = list()
    for index in range(args.worker_count):
        worker_id = str(index+1)
        workers.append(_start_worker_process(worker_id, rep_socket_uri))

    # loop until all handoffs have been accomplished
    log.debug("start handoffs")
    for work_entry in  _generate_segment_rows(raw_segment_rows):

        if halt_event.is_set():
            log.warn("breaking due to halt_event")
            break
    
        # block until we have a ready worker
        try:
            request = rep_socket.recv_pyobj()
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                log.warn("breaking due to halt_event")
                break
            raise

        if request["message-type"] == "start":
            log.info("{0} initial request".format(request["worker-id"]))
        elif request["handoff-successful"]:
            log.info("{0} handoff ({1}, {2}) successful".format(
                request["worker-id"], 
                request["unified-id"], 
                request["conjoined-part"]))
        else:
            log.error("{0} handoff ({1}, {2}) failed: {3}".format(
                request["worker-id"],
                request["unified-id"], 
                request["conjoined-part"],
                request["error-message"]))

        rep_socket.send_pyobj(work_entry)

    log.debug("end of handoffs")

    for worker in workers:
        poll_subprocess(worker)
        terminate_subprocess(worker)

    rep_socket.close()

