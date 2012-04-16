# -*- coding: utf-8 -*-
"""
cluster_repair_main.py

repair defective node data
"""
import logging
import os
import os.path
import signal
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from anti_entropy.cluster_repair.node_data_reader import generate_node_data

class ClusterRepairError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_repair_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

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

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "cluster_repair")
    event_push_client.info("program-start", "cluster_repair starts")  

    try:
        for result_dict in generate_node_data(halt_event):
            pass

    except KeyboardInterrupt:
        halt_event.set()
    except Exception as instance:
        log.exception(str(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return -3

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

