# -*- coding: utf-8 -*-
"""
node_data_reader_subprocess.py

read one node, pass results to stdout
"""
import logging
import os
import sys

from tools.standard_logging import initialize_logging
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

def _send_test_data():
    log = logging.getLogger("_send_test_data")
    for i in range(1000):
        log.info("sending record #{0}".format(i+1))
        entry = {"record-number" : i+1,
                 "action"        : None,	 
                 "part"          : None,	 
                 "result"   	 : None,
                 "data"          : b"\0\0\0",}
        store_sized_pickle(entry, sys.stdout.buffer)

def main():
    """
    main entry point
    """
    source_node_name = sys.argv[1]
    log_path = "{0}/nimbusio_cluster_repair_data_reader_{1}_to_{2}.log".format(
        os.environ["NIMBUSIO_LOG_DIR"], source_node_name, _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")

    log.info("program starts: reading from node {0}".format(source_node_name))

    try:
        _send_test_data()
    except Exception as instance:
        log.exception(instance)
        return -1

    log.info("terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())
