# -*- coding: utf-8 -*-
"""
pusher.py

a simple proess that pushes data at the diy_client
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
    topic = "test-topic"
    context = zmq.Context()

    push_socket = context.socket(zmq.PUSH)
    push_socket.setsockopt(zmq.LINGER, 1000)
    push_socket.connect(_diy_client_pull_address)

    sub_socket = context.socket(zmq.SUB)
    sub_socket.setsockopt(zmq.LINGER, 1000)
    sub_socket.connect(_diy_client_pub_address)

    message = {
        "message-type"  : "push-body",
        "client-topic"  : topic
    }

    sub_socket.setsockopt(zmq.SUBSCRIBE, topic)
    push_socket.send_json(message)

    topic = sub_socket.recv()
    print "topic =", topic

    assert sub_socket.rcvmore()
    message = sub_socket.recv_json()

    body_list = list()
    while sub_socket.rcvmore():
        body_list.append(sub_socket.recv())
    
    sub_socket.close()
    push_socket.close()
    context.term()

    return 0

if __name__ == "__main__":
    sys.exit(main())

