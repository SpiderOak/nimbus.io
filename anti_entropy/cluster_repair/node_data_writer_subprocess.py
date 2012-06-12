# -*- coding: utf-8 -*-
"""
node_data_writer_subprocess.py

read stdin, send updates to a node
"""
import gzip
import logging
import os
import sys
import uuid

import zmq

from tools.standard_logging import initialize_logging
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle
 
class ClusterRepairError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_client_tag = "anti-entropy-repair-%s" % (_local_node_name, )
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_data_writer_anti_entropy_addresses = \
        os.environ["NIMBUSIO_DATA_WRITER_ANTI_ENTROPY_ADDRESSES"].split()

def _process_repair_entries(index, dest_node_name, req_socket):
    return 0

def main():
    """
    main entry point
    """
    index_str = sys.argv[1]
    index = int(index_str)
    dest_node_name = _node_names[index]
    data_writer_anti_entropy_address = \
            _data_writer_anti_entropy_addresses[index]

    log_path = "{0}/nimbusio_cluster_repair_data_writer_{1}_to_{2}.log".format(
        os.environ["NIMBUSIO_LOG_DIR"], dest_node_name, _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")

    log.info("program starts: writing to node {0}".format(dest_node_name))

    zeromq_context = zmq.Context()

    req_socket = zeromq_context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting req socket to {0}".format(
        data_writer_anti_entropy_address))
    req_socket.connect(data_writer_anti_entropy_address)

    return_value = 0

    try:
        entries_processed = _process_repair_entries(index, 
                                                    dest_node_name, 
                                                    req_socket)
    except Exception as instance:
        log.exception(instance)
        return_value = 1
    else:
        log.info("terminates normally {0} entries processed".format(
            entries_processed))
    finally:
        req_socket.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

