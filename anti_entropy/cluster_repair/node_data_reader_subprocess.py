# -*- coding: utf-8 -*-
"""
node_data_reader_subprocess.py

read one node, pass results to stdout
"""
import gzip
import logging
import os
import sys
import uuid

import zmq

from tools.standard_logging import initialize_logging
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle
from tools.data_definitions import compute_expected_slice_count
 
from anti_entropy.anti_entropy_util import compute_data_repair_file_path, \
        anti_entropy_damaged_records

class ClusterRepairError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_client_tag = "anti-entropy-repair-%s" % (_local_node_name, )
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_data_reader_anti_entropy_addresses = \
        os.environ["NIMBUSIO_DATA_READER_ANTI_ENTROPY_ADDRESSES"].split()

def _get_sequence_from_data_reader(req_socket, segment_row, sequence_num):
    log = logging.getLogger("_get_sequence_from_data_reader")
    message_id = uuid.uuid1().hex
    message = {
        "message-type"              : "retrieve-segment-sequence",
        "client-tag"                : _client_tag,
        "message-id"                : message_id,
        "segment-unified-id"        : segment_row["unified_id"],
        "segment-conjoined-part"    : segment_row["conjoined_part"],
        "segment-num"               : segment_row["segment_num"],
        "sequence-num"              : sequence_num, }

    log.debug("sending retrieve-segment-sequence {0} {1} {2} {3}".format(
        segment_row["unified_id"], 
        segment_row["conjoined_part"],
        segment_row["segment_num"],
        sequence_num,
    ))

    req_socket.send_json(message)

    reply = req_socket.recv_json()
    body = []
    while req_socket.rcvmore:
        body.append(req_socket.recv())

    if reply["result"] != "success":
        error_message = "retrieve-segment-sequence failed {0}".format(
            reply["error-message"])
        raise ClusterRepairError(error_message)

    return body

def _compute_part_label(sequence_num, expected_slice_count):
    if sequence_num == 0:
        if expected_slice_count == 1:
            return "entire"
        return "start"
    if sequence_num == expected_slice_count-1:
        return "finish"
    return "next"

def _process_repair_entries(index, source_node_name, req_socket):
    log = logging.getLogger("_process_repair_entries")

    repair_file_path = compute_data_repair_file_path()
    log.debug("opening {0}".format(repair_file_path))
    repair_file = gzip.GzipFile(filename=repair_file_path, mode="rb")

    record_number = 0
    while True:
        try:
            row_key, segment_status, segment_data = \
                    retrieve_sized_pickle(repair_file)
        except EOFError:
            log.debug("EOF at record number {0}".format(record_number))
            repair_file.close()
            return record_number

        damaged_sequence_numbers = list()
        for segment_row in segment_data:
            damaged_sequence_numbers.extend(
                segment_row["damaged_sequence_numbers"])

        segment_row = segment_data[index]

        record_number += 1
        result = {"record_number"   : record_number,
                  "action"          : None,	 
                  "part"            : None,	 
                  "result"          : None,
                  "data"            : None,}

        expected_slice_count = \
            compute_expected_slice_count(segment_row["file_size"])

        for sequence_num in range(0, expected_slice_count):
            result["data"] = None
            if sequence_num in damaged_sequence_numbers:
                log.debug("{0} damaged sequence {1}".format(row_key,
                                                            sequence_num))
                result["action"] = "read"
                result["part"] = _compute_part_label(sequence_num, 
                                                     expected_slice_count)
                try:
                    data = _get_sequence_from_data_reader(req_socket, 
                                                          segment_row, 
                                                          sequence_num)
                except Exception as  instance:
                    log.exception("record #{0} sequence {1} {2}".format(
                        record_number, sequence_num, instance))
                    result["result"] = "error"
                else:
                    result["result"] = "success"
                    result["data"] = data
            else:
                result["action"] = "skip"
                result["result"] = None

            unified_id, conjoined_part = row_key
            sequence_key = (unified_id, 
                            conjoined_part, 
                            sequence_num, 
                            segment_row["segment_num"])
            log.debug("storing {0} {1}".format(sequence_key,
                                               result["action"]))
            store_sized_pickle((sequence_key, segment_status, result, ), 
                               sys.stdout.buffer)

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
        audit_records_processed = _process_repair_entries(index, 
                                                          source_node_name, 
                                                          req_socket)
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

