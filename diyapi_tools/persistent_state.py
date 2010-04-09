# -*- coding: utf-8 -*-
"""
persistent_state.py

common code for saving and loading stagte
"""
import logging
import cPickle
import os
import os.path

_repository_path = os.environ["PANDORA_REPOSITORY_PATH"]

def _state_path(name):
    return os.path.join(_repository_path, ".".join([name, "state", ]))

def save_state(state, name):
    """save a process's state to disk"""
    with open(_state_path(name), "w") as output_file:
        cPickle.dump(state, output_file)

def load_state(name):
    """
    return a state object loaded from disk, or None
    remove the state file immediately to avoid picking it up again
    """
    log = logging.getLogger("load_state")
    path = _state_path(name)
    result = None
    if os.path.exists(path):
        with open(path, "r") as input_file:
            try:
                result = cPickle.load(input_file)
            except Exception:
                log.exception("loading state")
            finally:
                try:
                    os.unlink(path)
                except Exception:
                    log.exception("removing state")
        
    return result

