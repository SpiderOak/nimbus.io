#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Ticket #47 Update/Invalidate memcache records when central DB changes
"""
import base64
import logging
import os
import pickle
import sys
from threading import Event
import time
import zlib

import memcache

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        prepare_ipc_path
from tools.process_util import set_signal_handler

_log_path = "{0}/nimbusio_central_cache_update.log".format(
    os.environ["NIMBUSIO_LOG_DIR"]) 
_skeeter_pub_socket_uri = os.environ["NIMBUSIO_CENTRAL_SKEETER_URI"]
_heartbeat_channel = "heartbeat"
_cache_update_channel = "nimbusio_central_cache_update"
_memcached_host = os.environ.get("NIMBUSIO_MEMCACHED_HOST", "localhost")
_memcached_port = int(os.environ.get("NIMBUSIO_MEMCACHED_PORT", "11211"))
_memcached_nodes = ["{0}:{1}".format(_memcached_host, _memcached_port), ]

def _create_sub_socket(zeromq_context):
    log = logging.getLogger("_create_pub_socket")
    if _skeeter_pub_socket_uri.startswith("ipc://"):
        prepare_ipc_path(_skeeter_pub_socket_uri)
    sub_socket = zeromq_context.socket(zmq.SUB)
    log.info("connecting to {0}".format(_skeeter_pub_socket_uri))
    sub_socket.connect(_skeeter_pub_socket_uri)
    log.info("subscribing to {0}".format(_heartbeat_channel))
    sub_socket.setsockopt(zmq.SUBSCRIBE, _heartbeat_channel)
    log.info("subscribing to {0}".format(_cache_update_channel))
    sub_socket.setsockopt(zmq.SUBSCRIBE, _cache_update_channel)

    return sub_socket

def _process_one_event(memcached_client, expected_sequence, topic, meta, data):
    log = logging.getLogger("event")

    meta_dict = dict()
    for entry in meta.strip().split(";"):
        [key, value] = entry.split("=")
        meta_dict[key.strip()] = value.strip()

    # every event should have a sequence and a timestamp
    meta_dict["timestamp"] = time.ctime(int(meta_dict["timestamp"]))
    meta_dict["sequence"] = int(meta_dict["sequence"])

    if topic == "heartbeat":
        connect_time = int(meta_dict["connected"])
        if connect_time == 0:
            connect_str = "*not connected*"
        else:
            connect_str = time.ctime(connect_time)
        line = "{0:30} {1:20} {2:8} connected={3}".format(
            meta_dict["timestamp"], 
            topic, 
            meta_dict["sequence"], 
            connect_str)
    else:
        table_name, raw_data = data.split("\n")
        try:
            update_data = \
                pickle.loads(zlib.decompress(base64.b64decode(raw_data)))
        except Exception:
            log.exception("unable to parse data '{0}'".format(raw_data))
            return 

        line = "{0:30} {1:20} {2:8} {3}".format(
            meta_dict["timestamp"], 
            topic, 
            meta_dict["sequence"], 
            update_data)

    log.info(line)

    if expected_sequence[topic] is None:
        expected_sequence[topic] = meta_dict["sequence"] + 1
    elif meta_dict["sequence"] == expected_sequence[topic]:
        expected_sequence[topic] += 1
    else:
        message = \
            "{0} out of sequence expected {1} found {2}".format(
                topic, expected_sequence[topic], meta_dict["sequence"])
        log.error(message)
        expected_sequence[topic] = None

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    memcached_client = memcache.Client(_memcached_nodes)

    zeromq_context = zmq.Context()
    sub_socket = _create_sub_socket(zeromq_context)

    expected_sequence = {
        _heartbeat_channel    : None,
        _cache_update_channel : None,
    }

    while not halt_event.is_set():
        try:
            topic = sub_socket.recv()
            assert sub_socket.rcvmore
            meta = sub_socket.recv()
            if sub_socket.rcvmore:
                data = sub_socket.recv()
            else:
                data = ""

            _process_one_event(memcached_client, 
                               expected_sequence, 
                               topic, 
                               meta, 
                               data)

        except KeyboardInterrupt: # convenience for testing
            log.info("keyboard interrupt: terminating normally")
            halt_event.set()
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                log.info("interrupted system call - ok at shutdown")
            else:
                log.exception("zeromq error processing request")
                return_value = 1
            halt_event.set()
        except Exception:
            log.exception("error processing request")
            return_value = 1
            halt_event.set()

    sub_socket.close()
    zeromq_context.term()

    log.info("program teminates: return value = {0}".format(return_value))
    return return_value

if __name__ == "__main__":
    sys.exit(main())

