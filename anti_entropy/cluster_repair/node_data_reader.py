# -*- coding: utf-8 -*-
"""
node_data_reader.py

manage 10 subprocesses to read data from nodes
"""
import errno
import logging
import os 
import subprocess
import sys

from tools.sized_pickle import retrieve_sized_pickle

class NodeDataReaderError(Exception):
    pass

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_read_buffer_size = int(
    os.environ.get("NIMBUSIO_ANTI_ENTROPY_READ_BUFFER_SIZE", 
                   str(10 * 1024 ** 2)))

_environment_list = ["PYTHONPATH",
                    "NIMBUSIO_LOG_DIR",
                    "NIMBUSIO_NODE_NAME", 
                    "NIMBUSIO_REPOSITORY_PATH", ]

from anti_entropy.anti_entropy_util import identify_program_dir

def generate_node_data(halt_event):
    """
    collate data from subprocesses pulling from nodes
    """
    log = logging.getLogger("generate_node_data")
    subprocesses = _start_subprocesses(halt_event)

    eof = False
    while not halt_event.is_set() and not eof:
        result_dict = dict()
        for node_name in _node_names:
            process = subprocesses[node_name]

            try:
                entry = retrieve_sized_pickle(process.stdout)
            except EOFError:
                process.wait()
                process.poll()

                if process.returncode != 0:
                    error_message = "subprocess {0} failed {1}".format(
                        node_name, process.returncode)
                    log.error(error_message)
                    raise NodeDataReaderError(error_message)

                log.info("EOF node {0}".format(node_name))
                eof = True
                break

            log.info("node {0} record_number {1}".format(
                node_name, entry["record-number"]))
            result_dict[node_name] = entry

        yield result_dict

    for process in subprocesses.values():
        try:
            process.terminate()
            process.wait()
        except OSError as instance:
            if instance.errno == errno.ESRCH:
                continue
            raise

def _start_subprocesses(halt_event):
    """
    start subprocesses
    """
    log = logging.getLogger("start_subprocesses")
    subprocesses = dict()

    environment = dict(
        [(key, os.environ[key], ) for key in _environment_list])

    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_reader_subprocess.py")

    for node_name in _node_names:

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return subprocesses

        log.info("starting subprocess {0}".format(node_name))
        args = [sys.executable, subprocess_path, node_name ]
        process = subprocess.Popen(args, 
                                   bufsize=_read_buffer_size,
                                   stdout=subprocess.PIPE, 
                                   env=environment)
        assert process is not None
        subprocesses[node_name] = process

    return subprocesses

