# -*- coding: utf-8 -*-
"""
node_data_reader.py

manage 10 subprocesses to read data from nodes
"""
import errno
import heapq
import itertools
import logging
import operator
import os 
import signal
import subprocess
import sys
from threading import Event

from tools.standard_logging import initialize_logging
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle

class NodeDataReaderError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_repair_data_reader_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_read_buffer_size = int(
    os.environ.get("NIMBUSIO_ANTI_ENTROPY_READ_BUFFER_SIZE", 
                   str(10 * 1024 ** 2)))

_environment_list = ["PYTHONPATH",
                    "NIMBUSIO_LOG_DIR",
                    "NIMBUSIO_LOG_LEVEL",
                    "NIMBUSIO_NODE_NAME", 
                    "NIMBUSIO_NODE_NAME_SEQ", 
                    "NIMBUSIO_DATA_READER_ANTI_ENTROPY_ADDRESSES",
                    "NIMBUSIO_REPOSITORY_PATH", ]

from anti_entropy.anti_entropy_util import identify_program_dir

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _node_generator(halt_event, node_name, node_subprocess):
    log = logging.getLogger(node_name)
    while not halt_event.is_set():
        try:
            yield retrieve_sized_pickle(node_subprocess.stdout)
        except EOFError:
            log.info("EOFError, assuming processing complete")
            break

    returncode = node_subprocess.poll()
    if returncode is None:
        log.warn("subprocess still running")
        node_subprocess.terminate()
    log.debug("waiting for subprocess to terminate")
    returncode = node_subprocess.wait()
    if returncode == 0:
        log.debug("subprocess terminated normally")
    else:
        log.warn("subprocess returned {0}".format(returncode))

def _start_subprocesses(halt_event):
    """
    start subprocesses
    """
    log = logging.getLogger("start_subprocesses")
    node_generators = list()

    environment = dict(
        [(key, os.environ[key], ) for key in _environment_list])

    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_reader_subprocess.py")

    for index, node_name in enumerate(_node_names):

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return node_generators

        log.info("starting subprocess {0}".format(node_name))
        args = [sys.executable, subprocess_path, str(index) ]
        process = subprocess.Popen(args, 
                                   bufsize=_read_buffer_size,
                                   stdout=subprocess.PIPE, 
                                   env=environment)
        assert process is not None
        node_generators.append(_node_generator(halt_event, node_name, process))

    return node_generators

def _manage_subprocesses(halt_event, merge_manager):
    log = logging.getLogger("_manage_subprocesses")
    while not halt_event.is_set():
        group_object = itertools.groupby(merge_manager, operator.itemgetter(0))
        for key, node_data_group in group_object:
            log.debug("found group {0}".format(key))
            store_sized_pickle(list(node_data_group), sys.stdout.buffer)

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
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    node_generators = _start_subprocesses(halt_event)
    merge_manager = heapq.merge(*node_generators)

    try:
        _manage_subprocesses(halt_event, merge_manager)
    except Exception as instance:
        log.exception(instance)
        return_value = 1

    return return_value

if __name__ == "__main__":
    sys.exit(main())


