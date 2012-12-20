# -*- coding: utf-8 -*-
"""
options.py

parse commandline options
"""
import argparse 
import os
import socket

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

def parse_commandline():
    """
    parse commandline options
    """
    parser = argparse.ArgumentParser(description="handoff client")
    parser.add_argument("-n", "--node-name", dest="node_name",
                        default=_local_node_name,
                        help="The node to process handoffs for")
    parser.add_argument("-w", "--worker-count", dest="worker_count",
                        type=int, default=3,
                        help="The number of worker processes to use.")
    parser.add_argument("--host-name", dest="host_name",
                        default=socket.gethostname(),
                        help="host name for worker PULL addresses")
    parser.add_argument("-p", "--base-port", dest="base_port",
                        type=int, default=10000,
                        help="Starting port number for worker PULL addresses")

    return parser.parse_args()

