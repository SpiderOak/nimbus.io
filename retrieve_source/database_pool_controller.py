# -*- coding: utf-8 -*-
"""
database_pool_controller.py

Manage a pool of database workers
"""

import os
import os.path
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall, \
        prepare_ipc_path
from tools.process_util import identify_program_dir, set_signal_handler

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_db_pool_controller_{1}.log"

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

    try:
        while not halt_event.is_set():
            halt_event.wait(10.0)
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
        database_pool_controller.terminate()
        database_pool_controller.wait()
        if database_pool_controller.returncode != 0:
            log.error("database_pool_controller ({0}) {1}".format(
                database_pool_controller.returncode,
                database_pool_controller.stderr.read()))
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

