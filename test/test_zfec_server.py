# -*- coding: utf-8 -*-
"""
test_zfec_server.py

A process to test the zfec_server by connecting to the socket and
running various transactons.
"""
import logging
import os
import sys
import uuid

import zmq

from tools.standard_logging import initialize_logging

_log_path = "{0}/test_zfec_server.log".format(os.environ["NIMBUSIO_LOG_DIR"])
_zfec_server_address = os.environ["NIMBUSIO_ZFEC_SERVER_ADDRESS"]

def _run_tests(req_socket):
    log = logging.getLogger("_run_tests")
    success_count = 0
    failure_count = 0

    test_data_size = 1024
    test_data = os.urandom(test_data_size)

    message = {"message-type" : "zfec-encode", }
    req_socket.send_json(message, zmq.SNDMORE)
    req_socket.send(test_data)

    reply = req_socket.recv_json()
    reply_data = list()
    while req_socket.rcvmore:
        reply_data.append(req_socket.recv())

    if reply["result"] == "success":
        success_count += 1
    else:
        log.error("test failed {0}".format(reply["error-message"]))
        failure_count += 1

    return success_count, failure_count

def main():
    """
    main entry point
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    zeromq_context = zmq.Context()

    req_socket = zeromq_context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting req socket to {0}".format(_zfec_server_address))
    req_socket.connect(_zfec_server_address)

    return_value = 0

    try:
        success_count, failure_count = _run_tests(req_socket)
    except Exception as instance:
        log.exception(instance)
        return_value = 1
    else:
        log.info("terminates normally {0} successes {1} failures".format(
            success_count, failure_count))
    finally:
        req_socket.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

