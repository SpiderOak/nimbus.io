# -*- coding: utf-8 -*-
"""
defragger.py
"""
import logging
import os
import signal
import sys
from threading import Event

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_defragger_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_defrag_check_interval = 300.0
_database_retry_interval = 300.0

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _defrag_pass(local_connection):
    """
    Make a single defrag pass
    return the number of bytes defragged
    """
    log = logging.getLogger("_defrag_pass")
    return 0

def main():
    """
    main entry point

    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    local_connection = None

    while not halt_event.is_set():

        # if we don't have an open database connection, get one
        if local_connection is None:
            try:
                local_connection = get_node_local_connection()
            except Exception as instance:
                log.exception("Exception connecting to database")
                halt_event.wait(_database_retry_interval)
                continue

        # try one defrag pass
        bytes_defragged = 0
        try:
            bytes_defragged = _defrag_pass(local_connection)
        except KeyboardInterrupt:
            halt_event.set()
        except Exception as instance:
            log.exception(str(instance))

        log.info("bytes defragged = {0}".format(bytes_defragged))

        # if we didn't do anythibng on this pass...
        if bytes_defragged == 0:

            # close the database connection
            if local_connection is not None:
                local_connection.close()
                local_connection = None

            # wait and try again
            try:
                halt_event.wait(_defrag_check_interval)
            except KeyboardInterrupt:
                halt_event.set()
                
    if local_connection is not None:
        local_connection.close()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

