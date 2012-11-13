#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
worker.py

worker subproces for handoff server
"""
import logging
import sys
from threading import Event

import zmq

from tools.process_util import set_signal_handler
from tools.standard_logging import _log_format_template
from tools.zeromq_util import is_interrupted_system_call

_socket_high_water_mark = 1000

def _initialize_logging_to_stderr():
    """
    we log to stderr because we assume we are being called by a process
    that pipes standard error
    """
    log_level = logging.WARN
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _process_handoff(halt_event, source_node_ids, segment_row):
    import random
    halt_event.wait(random.randint(1, 10))

def main():
    """
    main entry point
    return 0 on normal termination (exit code)
    """
    worker_id = sys.argv[1]
    rep_socket_uri = sys.argv[2]
    log = logging.getLogger("main")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context =  zmq.Context()

    req_socket = zeromq_context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.HWM, _socket_high_water_mark)
    req_socket.connect(rep_socket_uri)

    # notify our parent that we are ready to receive work
    request = {"message-type" : "start",
               "worker-id"    : worker_id}
    req_socket.send_pyobj(request)

    while not halt_event.is_set():
        try:
            source_node_ids, segment_row = req_socket.recv_pyobj()
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                break
            log.exception(str(zmq_error))
            sys.exit(1)

        log.warn("reveived message")

        request = {"message-type"         : "handoff-complete",
                   "worker-id"            : worker_id,
                   "handoff-successful"   : True,
                   "unified-id"           : segment_row["unified_id"],
                   "conjoined-part"       : segment_row["conjoined_part"],
                   "source-node-ids"      : source_node_ids,
                   "error-message"        : ""}

        try:
            _process_handoff(halt_event, source_node_ids, segment_row)
        except Exception as instance:
            log.exception(instance)
            request["handoff-successful"] = False
            request["error-message"] = str(instance)

        req_socket.send_pyobj(request)

    req_socket.close()
    zeromq_context.term()
    return 0

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    log = logging.getLogger("__main__")
    try:
        sys.exit(main())
    except Exception as instance:
        log.exception(instance)
        sys.exit(1)
        
