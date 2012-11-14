#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
worker.py

worker subproces for handoff server
"""
import logging
import os
import sys
from threading import Event

import zmq

from tools.process_util import set_signal_handler
from tools.standard_logging import initialize_logging, _log_format_template
from tools.zeromq_util import is_interrupted_system_call

#from handoff_client.forwarder_coroutine import forwarder_coroutine

_socket_high_water_mark = 1000
_log_path_template = "{0}/nimbusio_handoff_client_worker_{1:03}.log"

def _add_logging_to_stderr():
    """
    we log to stderr because we assume we are being called by a process
    that pipes standard error
    """
    log_level = logging.WARN
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    logging.root.addHandler(handler)

def _process_handoff(halt_event, source_node_ids, segment_row):
    log = logging.getLogger("_process_handoff")
    log.info("start ({0}, {1}) from {2}".format(segment_row["unified_id"],
                                                segment_row["conjoined_part"],
                                                source_node_ids))
    import random
    halt_event.wait(random.randint(1, 10))

    log.info("done  ({0}, {1}) from {2}".format(segment_row["unified_id"],
                                                segment_row["conjoined_part"],
                                                source_node_ids))

def main(worker_id, rep_socket_uri):
    """
    main entry point
    return 0 on normal termination (exit code)
    """
    log = logging.getLogger("main")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context =  zmq.Context()

    req_socket = zeromq_context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.HWM, _socket_high_water_mark)
    req_socket.connect(rep_socket_uri)

    log.info("sending 'start' message")
    # notify our parent that we are ready to receive work
    request = {"message-type" : "start",
               "worker-id"    : worker_id}
    req_socket.send_pyobj(request)

    log.info("starting message loop")
    while not halt_event.is_set():
        try:
            message = req_socket.recv_pyobj()
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                log.info("interrupted system call wiht halt_event_set")
                break
            log.exception(str(zmq_error))
            sys.exit(1)
        assert not req_socket.rcvmore

        if message["message-type"] == "stop":
            log.info("'stop' message received")
            break

        assert message["message-type"] == "work", message["message-type"]
        source_node_ids, segment_row = message["work-entry"]

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
    log.info("end message loop")

    req_socket.close()
    zeromq_context.term()
    log.info("program terminates")
    return 0

if __name__ == "__main__":
    worker_id = int(sys.argv[1])
    rep_socket_uri = sys.argv[2]
    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"],
                                         worker_id)
    initialize_logging(log_path)
    _add_logging_to_stderr()
    log = logging.getLogger("__main__")
    try:
        sys.exit(main(worker_id, rep_socket_uri))
    except Exception as instance:
        log.exception(instance)
        sys.exit(1)
        
