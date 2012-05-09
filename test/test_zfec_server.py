# -*- coding: utf-8 -*-
"""
test_zfec_server.py

A process to test the zfec_server by connecting to the socket and
running various transactons.
"""
import logging
import os
import random
import sys
import uuid

import zmq

from tools.standard_logging import initialize_logging
from tools.data_definitions import block_generator, incoming_slice_size

_log_path = "{0}/test_zfec_server.log".format(os.environ["NIMBUSIO_LOG_DIR"])
_zfec_server_address = os.environ["NIMBUSIO_ZFEC_SERVER_ADDRESS"]
_num_segments = 10
_min_segments = 8

def _contact_server(req_socket, request, request_data):
    req_socket.send_json(request, zmq.SNDMORE)

    for data_segment in request_data[:-1]:
        req_socket.send(data_segment, zmq.SNDMORE)
    req_socket.send(request_data[-1])

    reply = req_socket.recv_json()
    reply_data = list()
    while req_socket.rcvmore:
        reply_data.append(req_socket.recv())

    return reply, reply_data

def _run_test(req_socket, segment_size):
    """test a segment that doesn't need padding"""
    log = logging.getLogger("_run_test_{0}".format(segment_size))
    test_data = os.urandom(segment_size)

    encoded_blocks = list()
    for raw_block in block_generator(test_data):
        request = {"message-type" : "zfec-encode", }
        reply, reply_data = _contact_server(req_socket, request, [raw_block, ])
        if reply["result"] != "success":
            log.error("{0} failed {1}".format(request, reply["error-message"]))
            return False
        encoded_blocks.append((reply["padding-size"], reply_data, ))
        
    segment_numbers = range(1, _num_segments+1)

    test_segment_numbers = random.sample(segment_numbers, _min_segments)

    decoded_segments = list()
    for padding_size, encoded_block in encoded_blocks:
        request = {"message-type"    : "zfec-decode", 
                   "segment-numbers" : test_segment_numbers,
                   "padding-size"    : padding_size}
        test_segments = [encoded_block[n-1] for n in test_segment_numbers]
        reply, reply_data = _contact_server(req_socket, request, test_segments)
        if reply["result"] != "success":
            log.error("{0} failed {1}".format(request, reply["error-message"]))
            return False
        decoded_segments.append(reply_data[0])

    decoded_data = b"".join(decoded_segments)
    if decoded_data != test_data:
        log.error("decoded data does not match test data")
        return False

    return True

def test_padded_segment(self):
    """test a segment that needs padding"""
    segment_size = 10 * 1024 * 1024 - 1
    test_data = os.urandom(segment_size)
    segmenter = ZfecSegmenter(_min_segments, _num_segments)

    padding_size = segmenter.padding_size(test_data)
    encoded_segments = segmenter.encode(block_generator(test_data))
    
    segment_numbers = range(1, _num_segments+1)

    test_segment_numbers = random.sample(segment_numbers, _min_segments)
    test_segments = [encoded_segments[n-1] for n in test_segment_numbers]

    decoded_segments = segmenter.decode(
        test_segments, test_segment_numbers, padding_size
    )

    decoded_data = "".join(decoded_segments)
    self.assertTrue(decoded_data == test_data, len(decoded_data))

def _run_tests(req_socket):
    log = logging.getLogger("_run_tests")
    success_count = 0
    failure_count = 0

    for segment_size in [incoming_slice_size, incoming_slice_size-1, ]:
        if _run_test(req_socket, segment_size):
            success_count += 1
        else:
            failure_count += 1

    return success_count, failure_count

def main():
    """
    main entry point
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    zeromq_context = zmq.Context()

    req_socket = zeromq_context.socket(zmq.REQ)
    req_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting req socket to {0}".format(_zfec_server_address))
    req_socket.connect(_zfec_server_address)

    return_value = 0

    try:
        success_count, failure_count = _run_tests(req_socket)
    except Exception as instance:
        log.exception(instance)
        return_value = 1
    else:
        log.info("terminates normally {0} successes {1} failures".format(
            success_count, failure_count))
    finally:
        req_socket.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

