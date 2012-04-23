# -*- coding: utf-8 -*-
"""
node_data_reader_subprocess.py

read one node, pass results to stdout
"""
import gzip
import logging
import os
import sys

import zmq

from tools.standard_logging import initialize_logging
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle
 
from anti_entropy.anti_entropy_util import compute_data_repair_file_path, \
        anti_entropy_damaged_records

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_data_reader_anti_entropy_addresses = \
        os.environ["NIMBUSIO_DATA_READER_ANTI_ENTROPY_ADDRESSES"].split()

def _handle_damaged_records(audit_data, result):
    log = logging.getLogger("_handle_damaged_records")
    log.info("record #{0}".format(result["record-number"]))
    store_sized_pickle(result, sys.stdout.buffer)

_dispatch_table = {
    anti_entropy_damaged_records : _handle_damaged_records,
}

def _process_repair_entries(req_socket):
    log = logging.getLogger("_process_repair_entries")

    repair_file_path = compute_data_repair_file_path()
    log.debug("opening {0}".format(repair_file_path))
    repair_file = gzip.GzipFile(filename=repair_file_path, mode="rb")

    record_number = 0
    while True:
        try:
            audit_data = retrieve_sized_pickle(repair_file)
        except EOFError:
            repair_file.close()
            return record_number

        record_number += 1
        result = {"record-number" : record_number,
                 "action"        : None,	 
                 "part"          : None,	 
                 "result"   	 : None,
                 "data"          : None,}

        segment_status = audit_data["segment-status"]
        try:
            _dispatch_table[segment_status](audit_data, result)
        except KeyError:
            log.error("record #{0}: Unknown segment status {1}".format(
                result["record-number"], segment_status))
            result["action"] = "skip"
            result["result"] = "error"
            store_sized_pickle(result, sys.stdout.buffer)

def main():
    """
    main entry point
    """
    index_str = sys.argv[1]
    index = int(index_str)
    source_node_name = _node_names[index]
    data_reader_anti_entropy_address = \
            _data_reader_anti_entropy_addresses[index]

    log_path = "{0}/nimbusio_cluster_repair_data_reader_{1}_to_{2}.log".format(
        os.environ["NIMBUSIO_LOG_DIR"], source_node_name, _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")

    log.info("program starts: reading from node {0}".format(source_node_name))

    zeromq_context = zmq.Context()

    req_socket = zeromq_context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting req socket to {0}".format(
        data_reader_anti_entropy_address))
    req_socket.connect(data_reader_anti_entropy_address)

    return_value = 0

    try:
        audit_records_processed = _process_repair_entries(req_socket)
    except Exception as instance:
        log.exception(instance)
        return_value = 1
    else:
        log.info("terminates normally {0} audit records processed".format(
            audit_records_processed))
    finally:
        req_socket.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())
