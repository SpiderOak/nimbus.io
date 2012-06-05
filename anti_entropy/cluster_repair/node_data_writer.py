# -*- coding: utf-8 -*-
"""
node_data_writer.py

manage 10 subprocesses to write data to nodes
"""
import errno
import logging
import os 
import subprocess
import sys
from threading import Event

from tools.standard_logging import initialize_logging
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle
from tools.process_util import set_signal_handler

class NodeDataWriterError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_repair_data_writer_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()

from tools.process_util import identify_program_dir

def _start_subprocesses(halt_event):
    """
    start subprocesses
    """
    log = logging.getLogger("start_subprocesses")
    node_subprocesses = list()

    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_writer_subprocess.py")

    for index, node_name in enumerate(_node_names):

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return node_subprocesses

        log.info("starting subprocess {0}".format(node_name))
        args = [sys.executable, subprocess_path, str(index) ]
        process = subprocess.Popen(args, 
                                   stdin=subprocess.PIPE)
        assert process is not None
        node_subprocesses.append(process)

    return node_subprocesses

def main():
    """
    main entry point

    return 0 for success (exit code)
    """
    return_value = 0

    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    node_subprocesses = _start_subprocesses(halt_event)

    try:
        halt_event.wait()
    except Exception as instance:
        log.exception(instance)
        return_value = 1

    for node_subprocess in node_subprocesses:
        node_subprocess.terminate()

    return return_value

if __name__ == "__main__":
    sys.exit(main())


