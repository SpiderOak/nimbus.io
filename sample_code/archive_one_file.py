# -*- coding: utf-8 -*-
"""
send_one_file.py

a simple process that tells the diy_client to upload a file
"""
import logging
import sys
import time

import zmq

_diy_client_pull_address = "ipc:///tmp/diy-client-main-pull/socket"
_diy_client_pub_address = "ipc:///tmp/diy-client-main-pub/socket"

def main():
    """
    main entry point
    """
    path = sys.argv[1]
    key = path
    topic = key 
    context = zmq.Context()

    push_socket = context.socket(zmq.PUSH)
    push_socket.setsockopt(zmq.LINGER, 1000)
    push_socket.connect(_diy_client_pull_address)

    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.LINGER, 1000)
    sub_socket.connect(_diy_client_pub_address)

    message = {
        "message-type"  : "archive-file",
        "client-topic"  : topic,
        "key"           : key,
        "path"          : path,
    }

    push_socket.send_json(message, zmq.SNDMORE)
    push_socket.send("pork")

    sub_socket.setsockopt(zmq.SUBSCRIBE, topic)

    while True:
        topic = sub_socket.recv()

        assert sub_socket.rcvmore()
        message = sub_socket.recv_json()

        body_list = list()
        while sub_socket.rcvmore():
            body_list.append(sub_socket.recv())

        print message
        if message["completed"]:
            break
    
    sub_socket.close()
    push_socket.close()
    context.term()

    return 0

if __name__ == "__main__":
    sys.exit(main())


