# -*- coding: utf-8 -*-
"""
segment_puller.py

pull segment and damaged_segment rows from node local databases
"""
import logging
import os
import os.path
import shutil
import subprocess
import sys
import time

class SegmentPullerError(Exception):
    pass

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]      
_polling_interval = 1.0

from cluster_inspector.segment_auditor import SegmentAuditor

def _identify_program_dir(target_dir):
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_dir)
        if os.path.isdir(test_path):
            return test_path

    raise ValueError(
        "Can't find %s in PYTHONPATH '%s'" % (target_dir, python_path, )
    )

class SegmentPuller(object):
    """
    pull segment and damaged_segment rows from node local databases
    """
    def __init__(self, state):
        self._state = state
        self._work_dir = os.path.join(_repository_path, "cluster_inspector")
        self._pullers = dict()

    def start_pullers(self, halt_event):
        """
        start puller subprocesses
        This is a task for the time queue.
        """
        log = logging.getLogger("start_pullers")

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            return

        # if there's any wreckage from a previous run, clear it out
        if os.path.exists(self._work_dir):
            log.info("removing old {0}".format(self._work_dir))
            shutil.rmtree(self._work_dir)
        os.mkdir(self._work_dir)

        environment_list = ["PYTHONPATH",
                            "NIMBUSIO_LOG_DIR",
                            "NIMBUSIO_REPOSITORY_PATH",
                            "NIMBUSIO_NODE_NAME_SEQ",
                            "NIMBUSIO_NODE_DATABASE_HOSTS",
                            "NIMBUSIO_NODE_DATABASE_PORTS",
                            "NIMBUSIO_NODE_USER_PASSWORDS", 
                            "NIMBUSIO_NODE_NAME", ]
        environment = dict(
            [(key, os.environ[key], ) for key in environment_list])

        cluster_inspector_dir = _identify_program_dir("cluster_inspector")
        puller_path = os.path.join(cluster_inspector_dir,
                                   "segment_puller_subprocess.py")

        for index in range(len(_node_names)):
            log.info("starting subprocess {0}".format(_node_names[index]))
            args = [
                sys.executable,
                puller_path,
                self._work_dir,
                str(index)
            ]
            process = subprocess.Popen(args, 
                                       stderr=subprocess.PIPE, 
                                       env=environment)
            assert process is not None
            self._pullers[_node_names[index]] = process

        return [(self.poll_pullers, time.time() + _polling_interval, ), ]

    def poll_pullers(self, halt_event):
        """
        poll puller subprocesses
        This is a task for the time queue
        """
        log = logging.getLogger("poll_pullers")

        if halt_event.is_set():
            log.info("halt_event set: exiting")
            for node_name in self._pullers.keys():
                process = self._pullers[node_name]
                if process is None:
                    log.info("No process for {0}".format(node_name))
                    continue
                process.terminate()
                process.wait()
                return

        complete_count = 0

        for node_name in self._pullers.keys():
            process = self._pullers[node_name]
            if process is None:
                complete_count += 1
                continue

            process.poll()

            if process.returncode is None: # still running
                continue

            self._pullers[node_name] = None

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

        if complete_count < len(self._pullers):
            return [(self.poll_pullers, time.time() + _polling_interval, ), ]

        log.info("All processes complete, handing off to segment auditor")
        auditor = SegmentAuditor(self._state, self._work_dir)
        return [(auditor.run, time.time(), )]

