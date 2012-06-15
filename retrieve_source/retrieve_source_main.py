# -*- coding: utf-8 -*-
"""
retrieve_source_main.py

top level process for the retrieve_soruce set of processes
"""
import logging
import os
import os.path
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import PollError, \
        is_interrupted_system_call, \
        prepare_ipc_path
from tools.process_util import identify_program_dir, \
        set_signal_handler, \
        poll_subprocess, \
        terminate_subprocess
from tools.push_client import PUSHClient
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from retrieve_source.internal_sockets import internal_socket_uri_list, \
        db_controller_pull_socket_uri

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_retrieve_source_{1}.log"
_retrieve_source_address = os.environ["NIMBUSIO_DATA_READER_ADDRESS"]
_poll_timeout = 3.0

def _bind_rep_socket(zeromq_context):
    log = logging.getLogger("_bind_rep_socket")

    rep_socket = zeromq_context.socket(zmq.REP)
    rep_socket.setsockopt(zmq.LINGER, 1000)
    log.info("binding to {0}".format(_retrieve_source_address))
    rep_socket.bind(_retrieve_source_address)

    return rep_socket

def _connect_db_controller_push_client(zeromq_context):
    log = logging.getLogger("_connect_db_controller_push_client")
    db_controller_push_client = zeromq_context.socket(zmq.PUSH)
    db_controller_push_client.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting to {0}".format(db_controller_pull_socket_uri))
    db_controller_push_client.connect(db_controller_pull_socket_uri)

    return db_controller_push_client

def _launch_database_pool_controller():
    log = logging.getLogger("launch_database_pool_controller")
    module_dir = identify_program_dir("retrieve_source")
    module_path = os.path.join(module_dir, "database_pool_controller.py")
    
    args = [sys.executable, module_path, ]

    log.info("starting {0}".format(args))
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _launch_io_controller():
    log = logging.getLogger("launch_io_controller")
    module_dir = identify_program_dir("retrieve_source")
    module_path = os.path.join(module_dir, "io_controller.py")
    
    args = [sys.executable, module_path, ]

    log.info("starting {0}".format(args))
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _handle_resilient_server_handshake(message, client_pull_addresses):
    log = logging.getLogger("_handle_resilient_server_handshake")
    log.debug("{client-tag} {client-address}".format(**message))

    if message["client-tag"] in client_pull_addresses:
        log.debug("replacing client-tag {client-tag}".format(**message)) 
        del client_pull_addresses[message["client-tag"]]

    client_pull_addresses[message["client-tag"]] = message["client-address"]
    
def _handle_resilient_server_signoff(message, client_pull_addresses):
    log = logging.getLogger("_handle_resilient_server_signoff")
    try:
        del client_pull_addresses[message["client-tag"]]
    except KeyError:
        log.info("no such client-tag: {client-tag}".format(**message))
    else:
        log.info("removing address: {client-tag}".format(**message))

_dispatch_table = {
    "resilient-server-handshake" : _handle_resilient_server_handshake,
    "resilient-server-signoff"   : _handle_resilient_server_signoff,}

def _process_one_request(rep_socket, 
                         client_pull_addresses, 
                         db_controller_push_client):
    """
    This function reads a request message from our rep socket and
    sends an immediate ack.

    If the request is a handshake or a signoff from a web server client,
    we use the information to update our client_pull_addresses

    Otherwise, we insert the client_pull_address into the request and
    push the message on to the database pool controller
    """
    log = logging.getLogger("_process_one_request")

    request = rep_socket.recv_json()

    # we're not expecting any data from a retrieve request
    assert not rep_socket.rcvmore

    ack_message = {
        "message-type" : "resilient-server-ack",
        "message-id"   : request["message-id"],
        "incoming-type": request["message-type"],
        "accepted"     : None
    }

    push_request_to_db_controller = False

    if request["message-type"] in _dispatch_table:
        _dispatch_table[request["message-type"]](request, 
                                                 client_pull_addresses)
        ack_message["accepted"] = True
    elif not "client-tag" in request:
        log.error("receive: invalid message '{0}'".format(request))
        ack_message["accepted"] = False
    else:
        if request["client-tag"] in client_pull_addresses:
            ack_message["accepted"] = True
            push_request_to_db_controller = True
        else:
            log.error("No active client-tag {client-tag}".format(**request))
            ack_message["accepted"] = False

    rep_socket.send_json(ack_message)

    if push_request_to_db_controller:
        request["client-pull-address"] = \
                client_pull_addresses[request["client-tag"]]
        db_controller_push_client.send(request)

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

    for internal_socket_uri in internal_socket_uri_list:
        prepare_ipc_path(internal_socket_uri)

    halt_event = Event()
    set_signal_handler(halt_event)

    database_pool_controller = _launch_database_pool_controller()
    io_controller = _launch_io_controller()

    zeromq_context = zmq.Context()
    rep_socket = _bind_rep_socket(zeromq_context)
    db_controller_push_client = PUSHClient(zeromq_context, 
                                           db_controller_pull_socket_uri)
    event_push_client = EventPushClient(zeromq_context, "retrieve_source")

    # we poll the sockets for readability, we assume we can always
    # write to the push client sockets
    poller = zmq.Poller()
    poller.register(rep_socket, zmq.POLLIN | zmq.POLLERR)

    client_pull_addresses = dict()

    try:
        while not halt_event.is_set():
            poll_subprocess(database_pool_controller)
            poll_subprocess(io_controller)

            # we've only registered one socket, so we could use an 'if' here,
            # but this 'for' works ok and it has the same form as the other
            # places where we use poller
            for active_socket, event_flags in poller.poll(_poll_timeout):
                if event_flags & zmq.POLLERR:
                    error_message = \
                        "error flags from zmq {0}".format(active_socket)
                    log.error(error_message)
                    raise PollError(error_message) 

                assert active_socket is rep_socket

                _process_one_request(rep_socket, 
                                     client_pull_addresses,
                                     db_controller_push_client)

    except KeyboardInterrupt: # convenience for testing
        log.info("keyboard interrupt: terminating normally")
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error) and halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("zeromq error processing request")
            event_push_client.exception(unhandled_exception_topic,
                                        "zeromq_error",
                                        exctype="ZMQError")
            return_value = 1
    except Exception as instance:
        log.exception("error processing request")
        event_push_client.exception(unhandled_exception_topic,
                                    str(instance),
                                    exctype=instance.__class__.__name__)
        return_value = 1
    else:
        log.info("program teminates normally")
    finally:
        terminate_subprocess(database_pool_controller)
        terminate_subprocess(io_controller)
        rep_socket.close()
        db_controller_push_client.close()
        event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

