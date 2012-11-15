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
        terminate_subprocess
from tools.data_definitions import segment_status_final, \
        segment_status_tombstone, \

_socket_dir = os.environ["NIMBUSIO_SOCKET_DIR"]
_socket_high_water_mark = 1000
_polling_interval = 1.0

def _start_worker_process(worker_id, dest_node_name, rep_socket_uri):
    module_dir = identify_program_dir("handoff_client")
    module_path = os.path.join(module_dir, "worker.py")
    
    args = [sys.executable, 
            module_path, 
            worker_id, 
            dest_node_name,
            rep_socket_uri, ]
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _key_function(segment_row):
    return (segment_row[1]["unified_id"], segment_row[1]["conjoined_part"], )

def _generate_segment_rows(raw_segment_rows):
    """
    yield tuples of (source_node_ids, segment_row)
    where source_node_names is a list of the nodes where the segment can be 
    retrieved
    """
    # sort on (unified_id, conjoined_part) to bring pairs together
    raw_segment_rows.sort(key=_key_function)

    for (unified_id, conjoined_part, ), group in \
        itertools.groupby(raw_segment_rows, _key_function):
        segment_row_list = list(group)
        assert len(segment_row_list) > 0
        assert len(segment_row_list) < 3, str(len(segment_row_list))
        source_node_names = list()
        for source_node_name, segment_row in segment_row_list:
            source_node_names.append(source_node_name)
        yield (source_node_names, segment_row_list[0][1], )

def _process_tombstone(node_databases, source_node_names, segment_row):
    log = logging.getLogger("_process_tombstone")
    raise ValueError("Not Implemented")

def process_segment_rows(halt_event, 
                         zeromq_context, 
                         args, 
                         node_databases,
                         raw_segment_rows):
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
        workers.append(_start_worker_process(worker_id, 
                                             args.node_name, 
                                             rep_socket_uri))

    # loop until all handoffs have been accomplished
    log.debug("start handoffs")
    work_generator =  _generate_segment_rows(raw_segment_rows)
    pending_handoff_count = 0
    while not halt_event.is_set():

        # get a segment row to process. If we are at EOF, segment_row = None
        try:
            source_node_names, segment_row = next(work_generator)
        except StopIteration:
            if pending_handoff_count == 0:
                break
            else:
                source_node_names, segment_row = None, None

        # if we have a segment row, and it is a tombstone, we can act
        # directly on the node database(s) without sending it to a worker
        if segment_row is not None:
            if segment_row["status"] == segment_status_tombstone:
                _process_tombstone(node_databases, 
                                   source_node_names, 
                                   segment_row)
                continue
            assert segment_row["status"] == segment_status_final, \
                segment_row["status"]
    
        # at this point we eaither have a segment row in final status, or
        # None, indicating no more data
        # block until we have a ready worker
        try:
            request = rep_socket.recv_pyobj()
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                log.warn("breaking due to halt_event")
                break
            raise
        assert not rep_socket.rcvmore

        # see how the worker handled the previous segment (if any)
        if request["message-type"] == "start":
            log.info("{0} initial request".format(request["worker-id"]))
        elif request["handoff-successful"]:
            log.info("{0} handoff ({1}, {2}) successful".format(
                request["worker-id"], 
                request["unified-id"], 
                request["conjoined-part"]))
            assert pending_handoff_count > 0
            pending_handoff_count -= 1
        else:
            log.error("{0} handoff ({1}, {2}) failed: {3}".format(
                request["worker-id"],
                request["unified-id"], 
                request["conjoined-part"],
                request["error-message"]))
            assert pending_handoff_count > 0
            pending_handoff_count -= 1

        if segment_row is None:
            # if we have no more work, tell the worker to stop
            work_message = {"message-type"        : "stop"}
        else:
            # otherwise, send the segment to the worker 
            work_message = {"message-type"        : "work",
                            "source-node_names"   : source_node_names,
                            "segment-row"         : segment_row}
            pending_handoff_count += 1

        rep_socket.send_pyobj(work_message)

    log.debug("end of handoffs")

    for worker in workers:
        terminate_subprocess(worker)

    rep_socket.close()

