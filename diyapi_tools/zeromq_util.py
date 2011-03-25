# -*- coding: utf-8 -*-
"""
zeromq_util.py

Utility routines to support zeromq messsaging
"""
import logging
import os
import os.path

def prepare_ipc_path(address):
    """
    IPC sockets need an existing file for an address
    """
    log = logging.getLogger("prepare_ipc_path")
    path = address[len("ipc://"):]
    dir_name = os.path.dirname(path)
    if not os.path.exists(dir_name):
        log.debug("creating %r" % (dir_name, ))
        os.makedirs(dir_name)
    if not os.path.exists(path):
        log.debug("opening %r" % (path, ))
        with open(path, "w") as output_file:
            output_file.write("pork")

