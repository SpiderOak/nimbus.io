# -*- coding: utf-8 -*-
"""
cluster_inspector_maibn.py

pull segment and damaged_segment rows from node local databases
"""
import logging
import os
import os.path
import shutil
import signal
import sys
from threading import Event
import time

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from anti_entropy.cluster_inspector.segment_puller import \
        pull_segments_from_nodes
from anti_entropy.cluster_inspector.segment_auditor import audit_segments

class ClusterInspectorError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_inspector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]      
_work_dir = os.path.join(_repository_path, "cluster_inspector")

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

    event_push_client = EventPushClient(zmq_context, "cluster_inspector")
    event_push_client.info("program-start", "cluster_inspector starts")  

    # if there's any wreckage from a previous run, clear it out
    if os.path.exists(_work_dir):
        log.info("removing old {0}".format(_work_dir))
        shutil.rmtree(_work_dir)
    os.mkdir(_work_dir)

    try:
        pull_segments_from_nodes(halt_event, _work_dir)

        if halt_event.is_set():
            log.info("halt_event set (1): exiting")
            _terminate_pullers(pullers)
            return -1

        audit_segments(halt_event, _work_dir)

    except KeyboardInterrupt:
        halt_event.set()
        connection.rollback()
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


