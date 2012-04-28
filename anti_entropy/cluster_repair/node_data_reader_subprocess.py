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
 
from anti_entropy.anti_entropy_util import compute_data_repair_file_path, \
        anti_entropy_damaged_records

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_client_tag = "anti-entropy-repair-%s" % (_local_node_name, )
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_data_reader_anti_entropy_addresses = \
        os.environ["NIMBUSIO_DATA_READER_ANTI_ENTROPY_ADDRESSES"].split()

def _get_data_from_data_reader(message, req_socket):
    req_socket.send_json(message)

    control = req_socket.recv_json()
    body = []
    while req_socket.rcvmore:
        body.append(req_socket.recv())

    return control, body

def _handle_damaged_records(req_socket, segment_row, result):
    log = logging.getLogger("_handle_damaged_records")
    log.info("record #{0}".format(result["record_number"]))

    message_id = uuid.uuid1().hex
    message = {
        "message-type"              : "retrieve-key-start",
        "message-id"                : message_id,
        "client-tag"                : _client_tag,
        "anti-entropy"              : True,
        "segment-unified-id"        : segment_row["unified_id"],
        "segment-conjoined-part"    : segment_row["conjoined_part"],
        "segment-num"               : segment_row["segment_num"],
        "handoff-node-id"           : segment_row["handoff_node_id"],
        "block-offset"              : 0,
        "block-count"               : None,
    }

    log.debug("sending retrieve-key-start {0} {1} {2}".format(
        segment_row["unified_id"], 
        segment_row["conjoined_part"],
        segment_row["segment_num"]
    ))

    sequence = 0

    while True:
        reply, data_list = _get_data_from_data_reader(message, req_socket)

        assert reply["message-type"] == "retrieve-key-reply", reply
        if reply["result"] != "success":
            log.error(reply["error-message"])
            result["status"] = "error"
            store_sized_pickle(result, sys.stdout.buffer)
            # XXX review: just because we had an error reading one particular
            # segment sequence, doesn't mean all the sequences will fail.  

            # A large object could have its data split over multiple value files.
            # Some might be present, others might fail.  Disk or file system
            # corruption can mean that reads at one offset in the same value
            # file might fail, but other offsets succeed.
            
            # We should know how many sequences there should be, and try to
            # retrieve all of them, or only give up when we receive a type of
            # error response that indicates that no reads of any more segments
            # could be successful.

            # I tried to show this in the spec with the sample results from the
            # node data reader. Record 4 has an error reading the 2nd segment
            # sequence, but the data reader provides the next two segments.

            # So this function needs to explicitly know how many segment
            # sequences there should be and try to get all of them, not just go
            # until it gets an error.
            return

        if reply["completed"] and sequence == 0:
            result["sequence_num"] = 0
        else:
            result["sequence_num"] = sequence + 1

        result["status"] = "valid"
        result["data"] = data_list
        store_sized_pickle(result, sys.stdout.buffer)

        if reply["completed"]:
            break

        sequence += 1

        message_id = uuid.uuid1().hex
        message = {
            "message-type"              : "retrieve-key-next",
            "message-id"                : message_id,
            "client-tag"                : _client_tag,
            "anti-entropy"              : True,
            "segment-unified-id"        : segment_row["unified_id"],
            "segment-conjoined-part"    : segment_row["conjoined_part"],
            "segment-num"               : segment_row["segment_num"],
            "handoff-node-id"           : segment_row["handoff_node_id"],
            "block-offset"              : 0,
            "block-count"               : None,
        }


_dispatch_table = {
    anti_entropy_damaged_records : _handle_damaged_records,
}

def _process_repair_entries(source_node_name, req_socket):
    log = logging.getLogger("_process_repair_entries")

    repair_file_path = compute_data_repair_file_path()
    log.debug("opening {0}".format(repair_file_path))
    repair_file = gzip.GzipFile(filename=repair_file_path, mode="rb")

    record_number = 0
    while True:
        try:
            audit_data = retrieve_sized_pickle(repair_file)
        except EOFError:
            log.debug("EOF at record number {0}".format(record_number))
            repair_file.close()
            return record_number

        segment_data = audit_data["segment-data"][source_node_name]
        segment_row = segment_data["segment-row"]

        record_number += 1
        # XXX review: where's the "action" key?
        #     and I guess we're using a key called "status" instead of a key
        #     called "result" which is what the spec says.  I guess it's
        #     confusing to have a key of result inside a dict called result.  I
        #     don't really care about which name is used but I'd prefer we were
        #     consistent, and also change the spec if we're changing the
        #     structure.
        result = {"record_number"   : record_number,
                  "status"          : None,	 
                  "unified_id"      : segment_row["unified_id"],	 
                  "conjoined_part"  : segment_row["conjoined_part"],
                  "segment_num"     : segment_row["segment_num"],
                  "sequence_num"    : None,
                  "data"            : None,}

        # XXX review: as mentioned elsewhere, we need to be determining damage
        # on a per segment-sequeence basis, not for the segment as a whole.
        if segment_data["is-damaged"]:
            log.debug("is-damaged {0} {1} {2}".format(
                segment_row["unified_id"],	 
                segment_row["conjoined_part"],
                segment_row["segment_num"]))
            result["status"] = "damaged"
            store_sized_pickle(result, sys.stdout.buffer)
            continue

        segment_status = audit_data["segment-status"]

        # XXX review: this construct could catch KeyErrors raised by the
        # dispatch function and log them as Unknown segment status.  Maybe just
        # explicitly check the dispatch table to avoid this possibility?

        # Actually, I'm not sure if a dispatch table is the right thing here
        # anyway.  There's basically only one choice.  Either we need to
        # provide the data from this node (action=read), or we don't
        # (action=skip).  Not much point in a dispatch table of one entry.

        # The spec says we yield AT LEAST one entry FOR EACH record number in
        # the input file.  Yield an entry with action=skip if we decide data
        # from our node isn't needed.  Otherwise we yield one or more entries
        # with action=read.

        # Why does the spec say to yield results with action=skip at all,
        # instead of just omitting them entirely, and thus also being able to
        # omit the 'action' key also?  Because explicit is better than implicit
        # I guess.

        # so maybe we need a function here like
        # node_data_needed_for_repair() that determines if we do read
        # or skip.

        try:
            _dispatch_table[segment_status](req_socket, segment_row, result)
        except KeyError:
            log.error("record #{0}: Unknown segment status {1}".format(
                result["record-number"], segment_status))
            result["status"] = "error"
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
        audit_records_processed = _process_repair_entries(source_node_name, 
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
