# -*- coding: utf-8 -*-
"""
database_pool_workerer.py

One of a pool of database workers
"""
import logging
import os
import os.path
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall
from tools.process_util import set_signal_handler
from tools.event_push_client import EventPushClient, unhandled_exception_topic

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_db_pool_worker_{1}_{2}.log"

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0

    worker_number = int(sys.argv[1])

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         worker_number,
                                         _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    event_source_name = "rs_dbpool_worker_{0}".format(worker_number)
    event_push_client = EventPushClient(zeromq_context, event_source_name)

    try:
        while not halt_event.is_set():
            halt_event.wait(10)
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
        event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

