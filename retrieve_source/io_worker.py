# -*- coding: utf-8 -*-
"""
io_worker.py

One of a pool of io workers
"""
import logging
import os
import os.path
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall
from tools.process_util import set_signal_handler
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from retrieve_source.internal_sockets import io_controller_router_socket_uri

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_io_worker_{1}_{2}_{3}.log"

def _send_work_request(volume_name, dealer_socket):
    """
    start the work cycle by notifying the controller that we are available
    """
    log = logging.getLogger("_send_initial_work_request")
    log.debug("sending initial request")
    message = {"message-type" : "ready-for-work",
               "volume-name"  : volume_name,}
    dealer_socket.send_json(message)

def _process_request(dealer_socket):
    """
    Wait for a reply to our last message from the controller.
    """
    log = logging.getLogger("_process_one_transaction")
    log.debug("waiting work request")
    try:
        request = dealer_socket.recv_json()
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error):
            raise InterruptedSystemCall()
        raise
    assert dealer_socket.rcvmore

    sequence_row = dealer_socket.recv_pyobj()

    value_file_path = compute_value_file_path(_repository_path, 
                                              sequence_row["space_id"], 
                                              sequence_row["value_file_id"]) 

    with  open(value_file_path, "rb") as value_file:
        value_file.seek(sequence_row.value_file_offset)
        encoded_data = value_file.read(sequence_row["size"])

    assert len(encoded_data) == sequence_row["size"]

    encoded_block_list = list(encoded_block_generator(encoded_data))

    recompute = False
    if request["left-offset"] > 0:
        encoded_block_list = encoded_block_list[request["left-offset"]:]
        recompute = True
    if request["right-offset"] > 0:
        encoded_block_list = encoded_block_list[:-request["left-offset"]]
        recompute = True

    segment_size = sequence_row["size"]
    segment_adler32 = sequence_row["adler32"]
    segment_md5_digest = sequence_row["hash"]

    # if we chopped some blocks out of the data, we must recompute
    # the check values
    if recompute:
        segment_size = 0
        segment_adler32 = 0
        segment_md5 = hashlib.md5()
        for encoded_block in encoded_block_list:
            segment_size += len(encoded_block)
            segment_adler32 = zlib.adler32(encoded_block, segment_adler32) 
            segment_md5.update(encoded_block)
        segment_md5_digest = segment_md5.digest()

    reply = {
        "message-type"          : "retrieve-key-reply",
        "client-tag"            : request["client-tag"],
        "message-id"            : request["message-id"],
        "retrieve-id"           : request["retrieve-id"]
        "segment-unified-id"    : request["segment-unified-id"],
        "segment-num"           : request["segment-num"],
        "segment-size"          : segment_size,
        "zfec-padding-size"     : sequence_row["zfec_padding_size"],
        "segment-adler32"       : segment_adler32,
        "segment-md5-digest"    : b64encode(segment_md5_digest),
        "sequence-num"          : None,
        "completed"             : request["completed"],
        "result"                : result,
        "error-message"         : error_message,
    }

    client_pull_address = request["client-pull-address"]
        
def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0

    volume_name = sys.argv[1]
    worker_number = int(sys.argv[2])

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         volume_name,
                                         worker_number,
                                         _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    event_source_name = "rs_io_worker_{0}_{1}".format(volume_name, 
                                                      worker_number)
    event_push_client = EventPushClient(zeromq_context, event_source_name)

    dealer_socket = zeromq_context.socket(zmq.DEALER)
    dealer_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting to {0}".format(io_controller_router_socket_uri))
    dealer_socket.connect(io_controller_router_socket_uri)

    try:
        while not halt_event.is_set():
            _send_work_request(volume_name, dealer_socket)
            _process_request(dealer_socket)
    except InterruptedSystemCall:
        if halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("zeromq error processing request")
            event_push_client.exception(unhandled_exception_topic,
                                        "Interrupted zeromq system call",
                                        exctype="InterruptedSystemCall")
            return_value = 1
    except Exception as instance:
        log.exception("error processing request")
        event_push_client.exception(unhandled_exception_topic,
                                    str(instance),
                                    exctype=instance.__class__.__name__)
        return_value = 1
    else:
        log.info("program teminates normally")
    finally:
        dealer_socket.close()
        event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

