"""
retrieve_source_main.py

top level process for the retrieve_soruce set of processes
"""
import logging
import os
import signal
from threading import Event
import sys

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import prepare_ipc_path

class InterrupedSystemCall(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_retrieve_source_{1}.log"
_retrieve_source_address = os.environ["NIMBUSIO_DATA_READER_ADDRESS"]

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _bind_rep_socket(zeromq_context):
    log = logging.getLogger("_bind_rep_socket")

    # we need a valid path for IPC sockets
    if _retrieve_source_address.startswith("ipc://"):
        prepare_ipc_path(_retrieve_source_address)

    rep_socket = zeromq_context.socket(zmq.REP)
    rep_socket.setsockopt(zmq.LINGER, 1000)
    log.info("binding to {0}".format(_retrieve_source_address))
    rep_socket.bind(_retrieve_source_address)

    return rep_socket

def _handle_resilient_server_handshake(message, pull_addresses):
    log = logging.getLogger("_handle_resilient_server_handshake")
    log.debug("{client-tag} {client-address}".format(**message))

    if message["client-tag"] in pull_addresses:
        log.debug("replacing client-tag {client-tag}".format(**message)) 
        del pull_addresses[message["client-tag"]]

    pull_addresses[message["client-tag"]] = message["client-address"]
    
def _handle_resilient_server_signoff(message, pull_addresses):
    log = logging.getLogger("_handle_resilient_server_signoff")
    try:
        del pull_addresses[message["client-tag"]]
    except KeyError:
        log.info("no such client-tag: {client-tag}".format(**message))
    else:
        log.info("removing address: {client-tag}".format(**message))

_dispatch_table = {
    "resilient-server-handshake" : _handle_resilient_server_handshake,
    "resilient-server-signoff"   : _handle_resilient_server_signoff,}

def _push_to_database_control(request, _request_data):
    log = logging.getLogger("_push_to_database_control")
    log.info("{0}".format(request))

def _process_one_request(rep_socket, pull_addresses):
    log = logging.getLogger("_process_one_request")

    try:
        request = rep_socket.recv_json()
    except zmq.ZMQError as instance:
        if str(instance) == "Interrupted system call":
            raise InterrupedSystemCall()
        raise
    request_data = list()
    while rep_socket.rcvmore:
        request_data.append(rep_socket.recv())

    ack_message = {
        "message-type" : "resilient-server-ack",
        "message-id"   : request["message-id"],
        "incoming-type": request["message-type"],
        "accepted"     : None
    }

    if request["message-type"] in _dispatch_table:
        _dispatch_table[request["message-type"]](request, pull_addresses)
        ack_message["accepted"] = True
    elif not "client-tag" in request:
        log.error("receive: invalid message '{0}'".format(request))
        ack_message["accepted"] = False
    else:
        if request["client-tag"] in pull_addresses:
            _push_to_database_control(request, request_data)
            ack_message["accepted"] = True
        else:
            log.error("No active client-tag {client-tag}".format(**request))
            ack_message["accepted"] = False

    rep_socket.send_json(ack_message)

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
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    zeromq_context = zmq.Context()
    rep_socket = _bind_rep_socket(zeromq_context)
    pull_addresses = dict()

    try:
        while not halt_event.is_set():
            _process_one_request(rep_socket, pull_addresses)
    except InterrupedSystemCall:
        if halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("error processing request")
            return_value = 1
    except Exception:
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

