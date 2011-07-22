# -*- coding: utf-8 -*-
"""
puller.py

a process that retrieves archived files, probably archived by pusher
"""
from collections import deque
import json
import logging
import sys
import time

import zmq

_diy_client_pull_address = "ipc:///tmp/diy-client-main-pull/socket"
_diy_client_pub_address = "ipc:///tmp/diy-client-main-pub/socket"

def _initialize_logging():
    """initialize the log"""
    # define a Handler which writes to sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)-8s %(name)-20s %(message)s")
    console.setFormatter(formatter)
    
    logging.root.addHandler(console)

    logging.root.setLevel(logging.DEBUG)

def _parse_command_line():
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option(
        '-c', "--concurrent-retrieves", dest="concurrent_retrieves", type="int",
        help="number of concurrent retrieve requests"
    )
    parser.set_defaults(concurrent_retrieves=1)
    parser.add_option(
        '-t', "--total-retrieves", dest="total_retrieves", type="int",
        help="total retrieve requests"
    )
    parser.set_defaults(total_retrieves=1)
    parser.add_option(
        '-k', "--key-prefix", dest="key_prefix", type="string",
        help="prefix to key, also used as PUB/SUB topic"
    )
    parser.set_defaults(key_prefix="pusher")

    options, _ = parser.parse_args()
    return options

def main():
    """
    main entry point
    """
    _initialize_logging()
    log = logging.getLogger("main")
    options = _parse_command_line()
    context = zmq.Context()
    listmatch_topic = "listmatch-%s" % (options.key_prefix, )

    log.info("concurrent_retrieves = %s" % (options.concurrent_retrieves, ))
    log.info("total_retrieves = %s" % (options.total_retrieves, ))
    log.info("key_prefix = %s" % (options.key_prefix, ))

    push_socket = context.socket(zmq.PUSH)
    push_socket.setsockopt(zmq.LINGER, 1000)
    push_socket.connect(_diy_client_pull_address)

    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.LINGER, 1000)
    sub_socket.connect(_diy_client_pub_address)

    active_keys = dict()
    completed_retrieves = 0

    sub_socket.setsockopt(zmq.SUBSCRIBE, listmatch_topic)
    sub_socket.setsockopt(zmq.SUBSCRIBE, options.key_prefix)

    listmatch_message = {
        "message-type"  : "list-match",
        "client-topic"  : listmatch_topic,
        "prefix"        : options.key_prefix,
    }
    push_socket.send_json(listmatch_message)

    highest_known_key = ""
    key_deque = deque()

    while completed_retrieves < options.total_retrieves:
        while len(active_keys) < options.concurrent_retrieves \
        and completed_retrieves + len(active_keys) < options.total_retrieves \
        and len(key_deque) > 0:
            key = key_deque.popleft()
            message = {
                "message-type"  : "retrieve-blob",
                "client-topic"  : options.key_prefix,
                "key"           : key        
            }

            push_socket.send_json(message)
            active_keys[key] = time.time()
            log.debug("key %s started" % (key, ))

        topic = sub_socket.recv()

        assert sub_socket.rcvmore()
        message = sub_socket.recv_json()

        body_list = list()
        while sub_socket.rcvmore():
            body_list.append(sub_socket.recv())

        if topic == listmatch_topic:
            assert len(body_list) == 1, body_list
            key_list = body_list[0].split("\n")
            log.debug("listmatch %s entries" % (len(key_list), ))
            for key in sorted(key_list):
                if key > highest_known_key:
                    key_deque.append(key)
                    highest_known_key = key
        elif topic == options.key_prefix:            
            if message["completed"]:
                blob_size = sum([len(entry) for entry in body_list])
                start_time = active_keys.pop(message["key"])
                if message["status"] == "OK":
                    elapsed_time = time.time() - start_time
                    log.info("key %s completed %4d bytes per second" % (
                        message["key"],
                        int(blob_size / elapsed_time),
                    ))
                else:
                    log.info("key %s completed %s %s" % (
                        message["key"],
                        message["status"],
                        message["error-message"]
                    ))

            completed_retrieves += 1

    sub_socket.close()
    push_socket.close()
    context.term()

    return 0

if __name__ == "__main__":
    sys.exit(main())


