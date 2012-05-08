# -*- coding: utf-8 -*-
"""
zfec_server_main.py

A zeromq server to handle zfect encoding of data
We do this in a server so we can call it from Python 3.x programs.
This is a temporary expedient until zfec gets ported to Python 3
"""
import logging
import os
import signal
from threading import Event
import sys

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import prepare_ipc_path

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_zfec_server_{1}-{2}.log"
_zfec_server_address = os.environ["NIMBUSIO_ZFEC_SERVER_ADDRESS"]

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _bind_rep_socket(zeromq_context):
    log = logging.getLogger("_bind_rep_socket")

    # we need a valid path for IPC sockets
    if _zfec_server_address.startswith("ipc://"):
        prepare_ipc_path(_zfec_server_address)

    rep_socket = zeromq_context.socket(zmq.REP)
    rep_socket.setsockopt(zmq.LINGER, 1000)
    log.info("binding to {0}".format(_zfec_server_address))
    rep_socket.bind(_zfec_server_address)

    return rep_socket

_dispatch_table = {
}

def _process_one_request(rep_socket):
    log = logging.getLogger("_process_one_request")
    request = rep_socket.recv_json()
    request_data = list()
    while rep_socket.rcvmore:
        request_data.append(rep_socket.recv())

    reply_data = []
    if request["message-type"] in _dispatch_table:
        function = _dispatch_table[request["message-type"]]
        try:
            reply, reply_data = function(request, request_data)
        except Exception, instance:
            log.exception(request)
            reply = request.copy()
            reply["message-type"] = \
                    "-".join([request["message-type"], "reply"])
            reply["result"] = "exception"
            reply["error-message"] = str(instance)
    else:
        log.error("unknown message type '{0}'".format(request["message-type"]))
        reply = request.copy()
        reply["message-type"] = \
                "-".join([request["message-type"], "reply"])
        reply["result"] = "unknown-message-type"
        reply["error-message"] = "Unknown message-type"

    if len(reply_data) > 0:
        rep_socket.send_json(reply, zmq.SNDMORE)
        for segment in reply_data[:-1]:
            rep_socket.send(segment, zmq.SNDMORE)
        rep_socket.send(reply_data[-1])
    else:
        rep_socket.send_json(reply)

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0
    if len(sys.argv) == 1:
        server_number = 0
    else:
        server_number = int(sys.argv[1])

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         _local_node_name,
                                         server_number)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    zeromq_context = zmq.Context()
    rep_socket = _bind_rep_socket(zeromq_context)

    try:
        while not halt_event.is_set():
            _process_one_request(rep_socket)
    except Exception as instance:
        log.exception("error processing request")
        return_value = 1
    else:
        log.info("program teminates normally")
    finally:
        rep_socket.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

