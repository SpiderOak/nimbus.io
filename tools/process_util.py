# -*- coding: utf-8 -*-
"""
process_util.py

utility functions for managing processes
"""
import os
import os.path
import signal

def identify_program_dir(target_dir):
    """
    return the full path to the directory where a program resides
    raise ValueError if there is no such directory
    """
    python_path = os.environ["PYTHONPATH"]
    for work_path in python_path.split(os.pathsep):
        test_path = os.path.join(work_path, target_dir)
        if os.path.isdir(test_path):
            return test_path

    raise ValueError(
        "Can't find %s in PYTHONPATH '%s'" % (target_dir, python_path, )
    )

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def set_signal_handler(halt_event):
    """
    set a signal handler to set halt_event when SIGTERM is raised
    """
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

