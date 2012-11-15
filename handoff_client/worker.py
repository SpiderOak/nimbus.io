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
from tools.zeromq_util import ipc_socket_uri, \
        prepare_ipc_path, \
        is_interrupted_system_call

from handoff_client.forwarder_coroutine import forwarder_coroutine

_socket_dir = os.environ["NIMBUSIO_SOCKET_DIR"]
_socket_high_water_mark = 1000
_log_path_template = "{0}/nimbusio_handoff_client_worker_{1:03}.log"
_client_tag_template = "handoff_client_worker_{1:03}"

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()

_data_reader_addresses = \
    os.environ["NIMBUSIO_DATA_READER_ADDRESSES"].split()
_reader_address_dict = dict(zip(_node_names, _data_reader_addresses))

_data_writer_addresses = \
    os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"].split()
_writer_address_dict = dict(zip(_node_names, _data_writer_addresses))

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

def _process_handoff(zeromq_context, 
                     halt_event, 
                     pull_socket,
                     pull_socket_uri,
                     client_tag,
                     source_node_names, 
                     dest_node_name,
                     segment_row):
    log = logging.getLogger("_process_handoff")
    log.info("start ({0}, {1}) from {2}".format(segment_row["unified_id"],
                                                segment_row["conjoined_part"],
                                                source_node_names))
    # we pick the first source name, because it's easy, and because,
    # since that source responded to us first, it might have better 
    # response
    # TODO: switch over to the second source on error
    source_node_name = source_node_names[0]

    reader_socket = ReqSocket(zmq_context,
                              _reader_address_dict[source_node_name],
                              client_tag,
                              pull_socket_uri,
                              halt_event)

    writer_socket = ReqSocket(zmq_context,
                              _writer_address_dict[dest_node_name],
                              client_tag,
                              pull_socket_uri,
                              halt_event)

    forwarder = forwarder_coroutine(self._node_dict,
                                    segment_row, 
                                    writer_socket, 
                                    reader_socket)

    next(forwarder)

    while not halt_event.is_set():
        try:
            message = pull_socket.recv_json()
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                log.info("interrupted system call with halt_event_set")
                break
            log.exception(str(zmq_error))
            sys.exit(1)

        data = list()
        while pull_socket.rcvmore:
            data.append(pull_socket.recv())
        if len(data) == 0:
            data = None
        elif len(data) == 1:
            data = data[0]


    reader_socket.close()
    writer_socket.close()

    log.info("done  ({0}, {1}) from {2}".format(segment_row["unified_id"],
                                                segment_row["conjoined_part"],
                                                source_node_names))

def main(worker_id, dest_node_name, rep_socket_uri):
    """
    main entry point
    return 0 on normal termination (exit code)
    """
    log = logging.getLogger("main")

    client_tag = _client_tag_template.format(worker_id)
    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context =  zmq.Context()

    log.debug("creating pull socket")
    pull_socket_uri = \
        ipc_socket_uri(_socket_dir, 
                       dest_node_name,
                       "handoff_client_worker_{0:3}".format(worker_id))
    prepare_ipc_path(pull_socket_uri)

    pull_socket = zeromq_context.socket(zmq.PULL)
    pull_socket.setsockopt(zmq.HWM, _socket_high_water_mark)
    log.info("binding pull socket to {0}".format(pull_socket_uri))
    pull_socket.bind(pull_socket_uri)

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
        source_node_names, segment_row = message["work-entry"]

        request = {"message-type"         : "handoff-complete",
                   "worker-id"            : worker_id,
                   "handoff-successful"   : True,
                   "unified-id"           : segment_row["unified_id"],
                   "conjoined-part"       : segment_row["conjoined_part"],
                   "source-node-names"    : message["source-node-names"],
                   "error-message"        : ""}


        try:
            _process_handoff(zeromq_context, 
                             halt_event,
                             pull_socket,
                             pull_socket_uri,
                             client_tag,
                             message["source-node-names", 
                             dest_node_name,
                             message["segment-row"]
        except Exception as instance:
            log.exception(instance)
            request["handoff-successful"] = False
            request["error-message"] = str(instance)

        req_socket.send_pyobj(request)
    log.info("end message loop")

    pull_socket.close()
    req_socket.close()
    zeromq_context.term()
    log.info("program terminates")
    return 0

if __name__ == "__main__":
    worker_id = int(sys.argv[1])
    dest_node_name = sys.argv[2]
    rep_socket_uri = sys.argv[3]
    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"],
                                         worker_id)
    initialize_logging(log_path)
    _add_logging_to_stderr()
    log = logging.getLogger("__main__")
    try:
        sys.exit(main(worker_id, dest_node_name, rep_socket_uri))
    except Exception as instance:
        log.exception(instance)
        sys.exit(1)
        
