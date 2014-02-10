# -*- coding: utf-8 -*-
"""
zeromq_util.py

Utility routines to support zeromq messsaging
"""
import logging
import os
import os.path

import zmq

try:
    _snd_hwm = zmq.HWM 
    _rcv_hwm = zmq.HWM 
except AttributeError: 
    _snd_hwm = zmq.SNDHWM 
    _rcv_hwm = zmq.RCVHWM 
 

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
        log.debug("creating {0}".format(dir_name))
        os.makedirs(dir_name)
    if not os.path.exists(path):
        log.debug("opening {0}".format(path))
        with open(path, "w") as output_file:
            output_file.write("pork")


def set_send_hwm(s, value):
    """
    set the high water mark in a way that works with zmq 3 and zmq 4
    """
    s.setsockopt(_snd_hwm, value)

def set_receive_hwm(s, value):
    """
    set the high water mark in a way that works with zmq 3 and zmq 4
    """
    s.setsockopt(_rcv_hwm, value)
