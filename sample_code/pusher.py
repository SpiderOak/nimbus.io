# -*- coding: utf-8 -*-
"""
pusher.py

push a blob of data repeatedly to the diy client
designed for load testing
"""
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
        '-s', "--blob-size", dest="blob_size", type="int",
        help="size (in mb) of the blob to be sent"
    )
    parser.set_defaults(blob_size=1)
    parser.add_option(
        '-c', "--concurrent-archives", dest="concurrent_archives", type="int",
        help="number of concurrent archive requests"
    )
    parser.set_defaults(concurrent_archives=1)
    parser.add_option(
        '-t', "--total-archives", dest="total_archives", type="int",
        help="total archive requests"
    )
    parser.set_defaults(total_archives=1)
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

    log.info("blob_size = %smb" % (options.blob_size, ))
    log.info("concurrent_archives = %s" % (options.concurrent_archives, ))
    log.info("total_archives = %s" % (options.total_archives, ))
    log.info("key_prefix = %s" % (options.key_prefix, ))

    blob_size_in_bytes = options.blob_size * 1024 ^ 2

    # we're no deduping, so the blob can be anything
    blob = "-" * blob_size_in_bytes

    push_socket = context.socket(zmq.PUSH)
    push_socket.setsockopt(zmq.LINGER, 1000)
    push_socket.connect(_diy_client_pull_address)

    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.LINGER, 1000)
    sub_socket.connect(_diy_client_pub_address)
    
    archive_sequence = 0
    active_keys = dict()
    completed_archives = 0

    sub_socket.setsockopt(zmq.SUBSCRIBE, options.key_prefix)

    while completed_archives < options.total_archives:
        while len(active_keys) < options.concurrent_archives \
        and completed_archives + len(active_keys) < options.total_archives:
            archive_sequence += 1
            key = "%s-%08d" % (options.key_prefix, archive_sequence, )
            message = {
                "message-type"  : "archive-blob",
                "client-topic"  : options.key_prefix,
                "key"           : key        
            }

            push_socket.send_json(message, zmq.SNDMORE)
            push_socket.send(blob)
            active_keys[key] = time.time()
            log.debug("key %s started" % (key, ))

        _ = sub_socket.recv() # topic
        assert sub_socket.rcvmore()
        message = sub_socket.recv_json()

        body_list = list()
        while sub_socket.rcvmore():
            body_list.append(sub_socket.recv())

        if message["completed"]:
            start_time = active_keys.pop(message["key"])
            if message["status"] == "OK":
                elapsed_time = time.time() - start_time
                log.info("key %s completed %4d bytes per second" % (
                    message["key"],
                    int(blob_size_in_bytes / elapsed_time),
                ))
            else:
                log.info("key %s completed %s %s" % (
                    message["key"],
                    message["status"],
                    message["error-message"]
                ))

            completed_archives += 1
    
    sub_socket.close()
    push_socket.close()
    context.term()

    return 0

if __name__ == "__main__":
    sys.exit(main())

