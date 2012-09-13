# -*- coding: utf-8 -*-
"""
process_util.py

utility functions for managing processes
"""
import logging
import os
import os.path
import signal

class SubprocessFailure(Exception):
    """
    A subprocess has terminated unexpectedly
    """
    pass

def identify_program_dir(target_dir):
    """
    return the full path to the directory where a program resides
    raise ValueError if there is no such directory
    """
    # we are looking for ...<work-path>/tools/process_util.py
    work_path = os.path.dirname(os.path.dirname(__file__))
    test_path = os.path.join(work_path, target_dir)
    if not os.path.isdir(test_path):
        raise ValueError("Can't find {0}, {1}".format(target_dir, __file__, ))

    return test_path

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def set_signal_handler(halt_event):
    """
    set a signal handler to set halt_event when SIGTERM is raised
    """
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

def poll_subprocess(process):
    """
    poll a subprocess to see if it is still running
    raise SubprocessFailure if it is not
    """
    process.poll()
    if process.returncode is not None:
        message = "subprocess failed {0} {1}".format(process.returncode,
                                                     process.stderr.read())
        raise SubprocessFailure(message)

def terminate_subprocess(process):
    """
    terminate a subprocess if it is still running
    if termination is abnormal, report it
    """
    log = logging.getLogger("terminate_subprocess")
    if process.returncode is None:
        process.terminate()
        _, stderr_data = process.communicate()
    else:
        log.warn("process is already terminated")
        stderr_data = ""

    if process.returncode != 0:
        log.error("process ({0}) {1}".format(process.returncode,
                                             stderr_data))


