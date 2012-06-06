# -*- coding: utf-8 -*-
"""
database_pool_controller.py

Manage a pool of database workers
"""
import logging
import os
import os.path
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall
from tools.process_util import identify_program_dir, set_signal_handler
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from retrieve_source.internal_sockets import db_controller_pull_socket_uri

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_db_pool_controller_{1}.log"

def _process_one_request(pull_socket):
    log = logging.getLogger("_process_one_request")
    try:
        request = pull_socket.recv_json()
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error):
            raise InterruptedSystemCall()
        raise
    assert not pull_socket.rcvmore
    log.info("{0}".format(request))

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

    pull_socket = zeromq_context.socket(zmq.PULL)
    log.debug("binding to {0}".format(db_controller_pull_socket_uri))
    pull_socket.bind(db_controller_pull_socket_uri)

    event_push_client = EventPushClient(zeromq_context, "retrieve_source")

    try:
        while not halt_event.is_set():
            _process_one_request(pull_socket)
    except InterruptedSystemCall:
        if halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("zeromq error processing request")
            event_push_client.exception(unhandled_exception_topic,
                                        "Interrupted zeromq system call",
                                        exctype="InterruptedSystemCall")
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
        pull_socket.close()
        event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

