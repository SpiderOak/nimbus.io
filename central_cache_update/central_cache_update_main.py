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
from tools.data_definitions import memcached_central_key_template

_log_path = "{0}/nimbusio_central_cache_update.log".format(
    os.environ["NIMBUSIO_LOG_DIR"]) 
_skeeter_pub_socket_uri = os.environ["NIMBUSIO_CENTRAL_SKEETER_URI"]
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
    log.info("subscribing to {0}".format(_cache_update_channel))
    sub_socket.setsockopt(zmq.SUBSCRIBE, _cache_update_channel)

    return sub_socket

def _process_collection_event(memcached_client, event_data):
    log = logging.getLogger("_process_collection_event")
    if event_data["event"] != "UPDATE":
        log.error("unknown event {0}".format(event_data))
        return

    lookup_fields = ["id", "name", ]
    for lookup_field in lookup_fields:
        key = memcached_central_key_template.format(
            "collection", 
            lookup_field, 
            event_data["old"][lookup_field])
        result = memcached_client.delete(key)
        log.info("delete {0} result = {1}".format(key, result))

    # deleted collections have their name changed to have __deleted__$id__ at
    # the front so that they do not conflict with future collections.  in order
    # to clear caches even when we have mass updates.
    name = event_data['old']['name']
    if name.startswith("__deleted__"):
        undecorated_name = name[name.rindex("_") + 1:]
        key = memcached_central_key_template.format(
            "collection", "name", undecorated_name)
        result = memcached_client.delete(key)
        log.info("delete {0} result = {1}".format(key, result))

def _process_customer_event(memcached_client, event_data):
    log = logging.getLogger("_process_customer_event")
    if event_data["event"] != "UPDATE":
        log.error("unknown event {0}".format(event_data))
        return

    lookup_fields = ["id", "username", ]
    for lookup_field in lookup_fields:
        key = memcached_central_key_template.format(
            "customer", 
            lookup_field, 
            event_data["old"][lookup_field])
        result = memcached_client.delete(key)
        log.info("delete {0} result = {1}".format(key, result))

def _process_customer_key_event(memcached_client, event_data):
    log = logging.getLogger("_process_customer_key_event")
    if event_data["event"] != "UPDATE":
        log.error("unknown event {0}".format(event_data))
        return

    lookup_field = "id"
    key = memcached_central_key_template.format(
        "customer_key", 
        lookup_field, 
        event_data["old"][lookup_field])
    result = memcached_client.delete(key)
    log.info("delete {0} result = {1}".format(key, result))

_dispatch_table = {
    "collection"   : _process_collection_event,
    "customer"     : _process_customer_event,
    "customer_key" : _process_customer_key_event,
}

def _process_one_event(memcached_client, expected_sequence, topic, meta, data):
    log = logging.getLogger("event")

    meta_dict = dict()
    for entry in meta.strip().split(";"):
        [key, value] = entry.split("=")
        meta_dict[key.strip()] = value.strip()

    # every event should have a sequence and a timestamp
    meta_dict["timestamp"] = time.ctime(int(meta_dict["timestamp"]))
    meta_dict["sequence"] = int(meta_dict["sequence"])

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

    table_name, uuid, raw_data = data.split("\n", 2)
    try:
        event_data = \
            pickle.loads(zlib.decompress(base64.b64decode(raw_data)))
    except Exception:
        log.exception("unable to parse {0} data '{1}'".format(
            table_name, raw_data))
        return 

    if "table_name" not in event_data or \
        event_data["table_name"] not in _dispatch_table:
        log.error("invalid event data {0}".format(event_data))

    _dispatch_table[event_data["table_name"]](memcached_client, event_data)

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

