# -*- coding: utf-8 -*-
"""
cluster_repair_main.py

repair defective node data
"""
import logging
import os
import os.path
import signal
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle

from anti_entropy.anti_entropy_util import identify_program_dir

class ClusterRepairError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_repair_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)

_read_buffer_size = int(
    os.environ.get("NIMBUSIO_ANTI_ENTROPY_READ_BUFFER_SIZE", 
                   str(10 * 1024 ** 2)))

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _start_read_subprocess():
    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_reader.py")

    args = [sys.executable, subprocess_path, ]
    process = subprocess.Popen(args, 
                               bufsize=_read_buffer_size,
                               stdout=subprocess.PIPE)
    assert process is not None
    return process

def _start_write_subprocess():
    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_writer.py")

    args = [sys.executable, subprocess_path, ]
    process = subprocess.Popen(args, 
                               stdin=subprocess.PIPE)
    assert process is not None
    return process

def _repair_cluster(halt_event, read_subprocess, write_subprocess):
    log = logging.getLogger("_repair_cluster")
    while not halt_event.is_set():
        try:
            data = retrieve_sized_pickle(read_subprocess.stdout)
        except EOFError:
            log.info("EOFError on input; assuming process complete")
            break
        else:
            log.debug("got data")

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

    read_subprocess = _start_read_subprocess()
    write_subprocess = _start_write_subprocess()

    try:
        _repair_cluster(halt_event, read_subprocess, write_subprocess)
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
    finally:
        read_subprocess.terminate()
        write_subprocess.terminate()
        event_push_client.close()
        zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

