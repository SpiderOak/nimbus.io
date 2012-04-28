# -*- coding: utf-8 -*-
"""
cluster_repair_main.py

repair defective node data
"""

# XXX review: this process actually has the structure not of the program
# cluster repair, but of the program described as "Node Data Reader" in the
# spec, in the section about the process hierarhy.  It's job is to read across
# the streams from the 10 reader subprocesses, and merge them into batches, one
# per segment and sequence, and write those batches to stdout.

# I.e. it is a subprocess of the repair process -- the other subprocess being
# Node Data Writer.

# the top level cluster repair process has only these two subprocesses.  It
# does not manage the 20 node reader ard writer subprocesses directly.

# Suggest we "git mv " this program to be named node_data_reader_main and
# refactor accordingly (i.e. change name of logs, event notifications
# published, exceptions, etc.)

# Atually, how about something like this for a directory structure: (directory
# structure follows process hierarchy structure) AND we split off meta repair
# into its own program.

# anti_entropy/
# anti_entropy/node_inspector/
# anti_entropy/cluster_inspector/
# anti_entropy/cluster_repair/
# anti_entropy/cluster_repair/cluster_repair_meta_main.py 
# anti_entropy/cluster_repair/cluster_repair_data_main.py
# anti_entropy/cluster_repair/data_reader/data_reader_main.py
# anti_entropy/cluster_repair/data_reader/data_reader_subprocess.py
# anti_entropy/cluster_repair/data_writer/data_writer_main.py
# anti_entropy/cluster_repair/data_writer/data_writer_subprocess.py

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
            # XXX review: all you have to do here is write these batches to
            # stdout. processing them isn't this program's job.  
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

