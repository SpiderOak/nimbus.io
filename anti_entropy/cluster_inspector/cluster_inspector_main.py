# -*- coding: utf-8 -*-
"""
cluster_inspector_maibn.py

pull segment and damaged_segment rows from node local databases
"""
import gzip
import logging
import os
import os.path
import shutil
import signal
import subprocess
import sys
from threading import Event
import time

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from anti_entropy.cluster_inspector.work_generator import generate_work

class ClusterInspectorError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_inspector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]      
_work_dir = os.path.join(_repository_path, "cluster_inspector")
_environment_list = ["PYTHONPATH",
                    "NIMBUSIO_LOG_DIR",
                    "NIMBUSIO_REPOSITORY_PATH",
                    "NIMBUSIO_NODE_NAME_SEQ",
                    "NIMBUSIO_NODE_DATABASE_HOSTS",
                    "NIMBUSIO_NODE_DATABASE_PORTS",
                    "NIMBUSIO_NODE_USER_PASSWORDS", 
                    "NIMBUSIO_NODE_NAME", ]
_polling_interval = 1.0

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _identify_program_dir(target_dir):
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_dir)
        if os.path.isdir(test_path):
            return test_path

    raise ValueError(
        "Can't find %s in PYTHONPATH '%s'" % (target_dir, python_path, )
    )

def _start_pullers(halt_event):
    """
    start puller subprocesses
    """
    log = logging.getLogger("start_pullers")
    pullers = dict()

    environment = dict(
        [(key, os.environ[key], ) for key in _environment_list])

    anti_entropy_dir = _identify_program_dir("anti_entropy")
    puller_path = os.path.join(anti_entropy_dir,
                               "cluster_inspector",
                               "segment_puller_subprocess.py")

    for index in range(len(_node_names)):

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return pullers

        log.info("starting subprocess {0}".format(_node_names[index]))
        args = [sys.executable, puller_path, _work_dir, str(index) ]
        process = subprocess.Popen(args, 
                                   stderr=subprocess.PIPE, 
                                   env=environment)
        assert process is not None
        pullers[_node_names[index]] = process

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
        raise ClusterInspectorError(error_message)

    return complete_count

def _terminate_pullers(pullers):
    for node_name in pullers.keys():
        process = pullers[node_name]
        process.terminate()
        process.wait()

def _audit_segments():
    log = logging.getLogger("_audit_segments")
    for work_data in generate_work(_work_dir):
        count = 0
        for entry in work_data.values():
            if entry is not None:
                count += 1
        log.info("{0} {1} count = {2}".format(list(work_data.values())[0].unified_id,
                                              list(work_data.values())[0].conjoined_part,
                                              count))
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
        pullers = _start_pullers(halt_event)

        if halt_event.is_set():
            log.info("halt_event set (1): exiting")
            _terminate_pullers(pullers)
            return -1

        complete_count = 0
        while not halt_event.is_set():
            complete_count += _poll_pullers(pullers)
            if complete_count == len(_node_names):
                break
            halt_event.wait(_polling_interval)

        if halt_event.is_set():
            log.info("halt_event set (2): exiting")
            _terminate_pullers(pullers)
            return -2

        _audit_segments()

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


