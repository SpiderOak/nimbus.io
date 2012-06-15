# -*- coding: utf-8 -*-
"""
zeromq_util.py

Utility routines to support zeromq messsaging
"""
import logging
import os
import os.path

class PollError(Exception):
    pass

class InterruptedSystemCall(Exception):
    pass

def is_interrupted_system_call(zmq_error):
    """
    predicate to test a zmq.ZMQError object for interrupted system call 
    This normally occurs when a process is terminated while blocking on
    a ZMQ socket. It's an error we generally choose to ignore.
    """
    return str(zmq_error) == "Interrupted system call"

def ipc_socket_uri(socket_path, node_name, socket_name):
    return "ipc:///{0}/{1}.{2}.socket".format(socket_path,
                                              node_name,
                                              socket_name)

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

