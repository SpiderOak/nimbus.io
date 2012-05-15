# -*- coding: utf-8 -*-
"""
cluster_repair_main.py

repair defective node data
"""
import logging
import os
import os.path
import signal
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.sized_pickle import store_sized_pickle, retrieve_sized_pickle
from tools.data_definitions import min_node_count, block_generator, \
        zfec_padding_size, \
        encoded_block_generator, \
        compute_expected_slice_count, \
        encoded_block_slice_size

from anti_entropy.anti_entropy_util import identify_program_dir

class ClusterRepairError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_cluster_repair_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name)

_read_buffer_size = int(
    os.environ.get("NIMBUSIO_ANTI_ENTROPY_READ_BUFFER_SIZE", 
                   str(10 * 1024 ** 2)))
_zfec_server_address = os.environ["NIMBUSIO_ZFEC_SERVER_ADDRESS"]

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _start_read_subprocess():
    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_reader.py")

    args = [sys.executable, subprocess_path, ]
    process = subprocess.Popen(args, 
                               bufsize=_read_buffer_size,
                               stdout=subprocess.PIPE)
    assert process is not None
    return process

def _start_write_subprocess():
    anti_entropy_dir = identify_program_dir("anti_entropy")
    subprocess_path = os.path.join(anti_entropy_dir,
                               "cluster_repair",
                               "node_data_writer.py")

    args = [sys.executable, subprocess_path, ]
    process = subprocess.Popen(args, 
                               stdin=subprocess.PIPE)
    assert process is not None
    return process

def _rebuild_sequence(zfec_server_req_socket, group_dict, write_subprocess):
    log = logging.getLogger("_rebuild_sequence")
    defective_nodes = list()
    good_segment_nums = list()
    block_lists = list()
    block_list_length_set = set()
    zfec_padding_size_set = set()
    for node_entry in group_dict["node_data"]:
        if node_entry["result"] == "success":
            assert node_entry["data"] is not None
            block_list_length_set.add(len(node_entry["data"]))
            zfec_padding_size_set.add(node_entry["zfec_padding_size"])
            block_lists.append(node_entry["data"])
            good_segment_nums.append(node_entry["segment_num"])
        else:
            defective_nodes.append(node_entry)

    # if we don't have any defective nodes, something is wrong
    if len(defective_nodes) == 0:
        log.error("no defective nodes found")
        return

    # if we don't have enough good nodes to rebuild the sequence,
    # we can't do anything
    if len(block_lists) < min_node_count:
        log.error("too few nodes ({0}) to rebuild sequence".format(
            len(block_lists)))
        return

    # if the block lists aren't all the same size, something is badly wrong
    if len(block_list_length_set) != 1:
        log.error("inconsistent size of blocks lists {0}".format(
            block_list_length_set))
        return
    block_list_length = block_list_length_set.pop()

    # if zfec_padding aren't all the same size, something is badly wrong
    if len(zfec_padding_size_set) != 1:
        log.error("inconsistent padding of data blocks {0}".format(
            zfec_padding_size_set))
        return
    final_zfec_padding_size = zfec_padding_size_set.pop()

    block_lists = block_lists[:min_node_count]
    good_segment_nums = good_segment_nums[:min_node_count]

    slice_count = 0
    decoded_blocks = list()
    for encoded_blocks in zip(*block_lists):
        slice_count += 1
        if slice_count == block_list_length:
            padding_size = final_zfec_padding_size
        else:
            padding_size = 0

        request = {"message-type"    : "zfec-decode", 
                   "segment-numbers" : good_segment_nums,
                   "padding-size"    : padding_size}

        log.debug("sending zfec-decode {0}".format(slice_count))
        zfec_server_req_socket.send_json(request, zmq.SNDMORE)

        for data_segment in encoded_blocks[:-1]:
            zfec_server_req_socket.send(data_segment, zmq.SNDMORE)
        zfec_server_req_socket.send(encoded_blocks[-1])

        log.debug("waiting reply")
        reply = zfec_server_req_socket.recv_json()
        
        reply_data = list()
        while zfec_server_req_socket.rcvmore:
            reply_data.append(zfec_server_req_socket.recv())

        assert reply["result"] == "success"
        assert len(reply_data) == 1

        decoded_blocks.append(reply_data[0])

def _repair_one_sequence(zfec_server_req_socket, group_dict, write_subprocess):
    log = logging.getLogger("_repair_one_sequence")

    # all the data_reader subprocesses should agree on the action
    action_set = set([n["action"] for n in group_dict["node_data"]])
    if len(action_set) != 1:
        error_message = \
                "ambiguous action {0}: unified_id={0}, " \
                "conjoined_part={1}, sequence_num={2}, " \
                "segment_status={3}".format(action_set,
                                            group_dict["unified_id"], 
                                            group_dict["conjoined_part"],
                                            group_dict["sequence_num"],
                                            group_dict["segment_status"])
        log.error(error_message)
        raise ClusterRepairError(error_message)

    action = list(action_set)[0]

    log.debug(
        "action={0} unified_id={1}, conjoined_part={2}, sequence_num={3}, " \
        "segment_status={4}".format(action,
                                    group_dict["unified_id"], 
                                    group_dict["conjoined_part"],
                                    group_dict["sequence_num"],
                                    group_dict["segment_status"]))

    if action == "skip":
        return

    if action == "read":
        _rebuild_sequence(zfec_server_req_socket, group_dict, write_subprocess)
        return

    log.error("Unknown action '{0}'".format(action))

def _repair_cluster(halt_event, 
                    zfec_server_req_socket, 
                    read_subprocess, 
                    write_subprocess):
    log = logging.getLogger("_repair_cluster")
    while not halt_event.is_set():
        try:
            group_dict = retrieve_sized_pickle(read_subprocess.stdout)
        except EOFError:
            log.info("EOFError on input; assuming process complete")
            break
        _repair_one_sequence(zfec_server_req_socket, 
                             group_dict, 
                             write_subprocess)

def main():
    """
    main entry point

    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "cluster_repair")
    event_push_client.info("program-start", "cluster_repair starts")  

    zfec_server_req_socket = zmq_context.socket(zmq.REQ)
    zfec_server_req_socket.setsockopt(zmq.LINGER, 1000)
    log.info("connecting req socket to {0}".format(_zfec_server_address))
    zfec_server_req_socket.connect(_zfec_server_address)

    read_subprocess = _start_read_subprocess()
    write_subprocess = _start_write_subprocess()

    try:
        _repair_cluster(halt_event, 
                        zfec_server_req_socket, 
                        read_subprocess, 
                        write_subprocess)
    except KeyboardInterrupt:
        halt_event.set()
    except Exception as instance:
        log.exception(str(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return -3
    finally:
        read_subprocess.terminate()
        write_subprocess.terminate()
        event_push_client.close()
        zfec_server_req_socket.close()
        zmq_context.term()

    log.info("program terminates normally")
    return 0

if __name__ == "__main__":
    sys.exit(main())

