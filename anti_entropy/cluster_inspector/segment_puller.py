# -*- coding: utf-8 -*-
"""
segment_puller.py

pull segments from remote nodes
"""
import logging
import os
import subprocess
import sys

from anti_entropy.anti_entropy_util import identify_program_dir

class SegmentPullerError(Exception):
    pass

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_environment_list = ["PYTHONPATH",
                    "NIMBUSIO_LOG_DIR",
                    "NIMBUSIO_REPOSITORY_PATH",
                    "NIMBUSIO_NODE_NAME_SEQ",
                    "NIMBUSIO_NODE_DATABASE_HOSTS",
                    "NIMBUSIO_NODE_DATABASE_PORTS",
                    "NIMBUSIO_NODE_USER_PASSWORDS", 
                    "NIMBUSIO_NODE_NAME", ]
_polling_interval = 1.0

def _start_pullers(halt_event, work_dir):
    """
    start puller subprocesses
    """
    log = logging.getLogger("start_pullers")
    pullers = dict()

    environment = dict(
        [(key, os.environ[key], ) for key in _environment_list])

    anti_entropy_dir = identify_program_dir("anti_entropy")
    puller_path = os.path.join(anti_entropy_dir,
                               "cluster_inspector",
                               "segment_puller_subprocess.py")

    for index, node_name in enumerate(_node_names):

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return pullers

        log.info("starting subprocess {0}".format(node_name))
        args = [sys.executable, puller_path, work_dir, str(index) ]
        process = subprocess.Popen(args, 
                                   stderr=subprocess.PIPE, 
                                   env=environment)
        assert process is not None
        pullers[node_name] = process

    return pullers

def _poll_pullers(pullers):
    """
    poll puller subprocesses
    """
    log = logging.getLogger("poll_pullers")

    complete_count = 0

    for node_name in pullers.keys():
        process = pullers[node_name]
        if process is None:
            continue

        process.poll()

        if process.returncode is None: # still running
            continue

        pullers[node_name] = None

        if process.returncode == 0:
            log.info("process for {0} terminated normally".format(
                node_name))
            complete_count += 1
            continue

        if process.stderr is not None:
            error = process.stderr.read()
        else:
            error = ""
        error_message = "subprocess {0} failed {1} {2}".format(
            node_name, process.returncode, error)
        log.error(error_message)
        raise SegmentPullerError(error_message)

    return complete_count

def _terminate_pullers(pullers):
    for node_name in pullers.keys():
        process = pullers[node_name]
        process.terminate()
        process.wait()

def pull_segments_from_nodes(halt_event, work_dir):
    """
    spawn subprocesses to request segment rows from nodes
    """
    pullers = _start_pullers(halt_event, work_dir)

    if halt_event.is_set():
        log.info("halt_event set (1): exiting")
        _terminate_pullers(pullers)
        return

    complete_count = 0
    while not halt_event.is_set():
        complete_count += _poll_pullers(pullers)
        if complete_count == len(_node_names):
            break
        halt_event.wait(_polling_interval)

