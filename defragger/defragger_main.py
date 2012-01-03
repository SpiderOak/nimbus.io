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

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_defragger_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_defrag_check_interval = 300.0

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _defrag_pass():
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

    while not halt_event.is_set():
        bytes_defragged = 0
        try:
            bytes_defragged = _defrag()
        except KeyboardInterrupt:
            halt_event.set()
        except Exception as instance:
            log.exception(str(instance))

        log.info("bytes defragged = {0}".format(bytes_defragged))

        if bytes_defragged == 0:
            try:
                halt_event.wait(_defrag_check_interval)
            except KeyboardInterrupt:
                halt_event.set()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())
