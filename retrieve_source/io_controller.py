# -*- coding: utf-8 -*-
"""
io_controller.py

Manage a pool of IO workers
"""
from collections import defaultdict, deque, namedtuple
import logging
import os
import os.path
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection
from tools.process_util import identify_program_dir, \
        set_signal_handler, \
        poll_subprocess, \
        terminate_subprocess
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.file_space import load_file_space_info, file_space_sanity_check
from tools.zeromq_util import PollError, \
        is_interrupted_system_call

from retrieve_source.internal_sockets import io_controller_pull_socket_uri, \
        io_controller_router_socket_uri

_resources_tuple = namedtuple("Resources", 
                              ["halt_event",
                               "volume_by_space_id",
                               "pull_socket",
                               "router_socket",
                               "event_push_client",
                               "pending_work_by_volume",
                               "available_ident_by_volume",])

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_io_controller_{1}.log"
_repository_path = os.environ["NIMBUSIO_REPOSITORY_PATH"]
_worker_count = int(os.environ.get("NIMBUSIO_RETRIEVE_IO_WORKER_COUNT", "2"))
_poll_timeout = 3.0 

def _launch_io_worker(volume_name, worker_number):
    log = logging.getLogger("launch_io_worker")
    module_dir = identify_program_dir("retrieve_source")
    module_path = os.path.join(module_dir, "io_worker.py")
    
    args = [sys.executable, module_path, volume_name, str(worker_number), ]

    log.info("starting {0}".format(args))
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _send_pending_work_to_available_workers(resources):
    """
    send messages from the pending_work_queue 
    to workers in the available_ident_queue
    """
    log = logging.getLogger("_send_pending_work_to_available_workers")
    for volume_name in set(resources.volume_by_space_id.values()):
        work_count = min(len(resources.pending_work_by_volume[volume_name]), 
                         len(resources.available_ident_by_volume[volume_name]))
        for _ in range(work_count):
            message, sequence_row = \
                resources.pending_work_by_volume[volume_name].popleft()
            ident = resources.available_ident_by_volume[volume_name].popleft()
            resources.router_socket.send(ident, zmq.SNDMORE)
            resources.router_socket.send_json(message, zmq.SNDMORE)
            resources.router_socket.send_pyobj(sequence_row)

def _read_pull_socket(resources):
    """
    read messages from the PULL socket until we would block
    """
    log = logging.getLogger("_read_pull_socket")

    while True: # read until we would block
        try:
            message = resources.pull_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError as instance:
            if instance.errno == zmq.EAGAIN:
                break
            raise

        assert resources.pull_socket.rcvmore
        sequence_row = resources.pull_socket.recv_pyobj()

        space_id = sequence_row["space_id"]
        try:
            volume_name = resources.volume_by_space_id[space_id]
        except KeyError:
            error_message = "Unknown space_id {0} {1}".format(space_id,
                                                              message)
            log.error(error_message)
            resources.event_push_client.error("unknown_space_id",
                                              error_message)
            return

        log.debug("work for volume {0} {1}".format(volume_name, sequence_row))
        resources.pending_work_by_volume[volume_name].append((message,
                                                             sequence_row, ))

    _send_pending_work_to_available_workers(resources)

def _read_router_socket(resources):
    """
    read a message from the router socket (from one of our worker processes)
    if the message-type is 'ready-for-work' (initial message)
        add the message ident to the resources.available_ident_queue
    """
    log = logging.getLogger("_read_router_socket")

    ident = resources.router_socket.recv()
    assert resources.router_socket.rcvmore
    message = resources.router_socket.recv_json()
    assert not resources.router_socket.rcvmore
    assert message["message-type"] == "ready-for-work"

    resources.available_ident_by_volume[message["volume-name"]].append(ident) 
    _send_pending_work_to_available_workers(resources)

def _volume_name_by_space_id():
    """
    The control process creates a pool of worker processes of configurable size
    (default 2) for each distinct file space. However, if multiple file spaces 
    have the same "volume name" value, then one worker process pool handles 
    read requests to all of the file spaces with that same volume name. 
    In other words, there will be a pool of workers for each non null volume 
    name. Null values are never the same as other null values, so if no volume 
    names are specified for the table spaces, there will be one read worker 
    pool per file space.

    So we assign a volume name to each space_id, creating a 'null-nn' name
    if volume is null
    """
    connection =  get_node_local_connection()
    file_space_info = load_file_space_info(connection)
    connection.close()
    file_space_sanity_check(file_space_info, _repository_path)

    volume_name_by_space_id = dict() 
    null_count = 0
    for file_space_row_list in file_space_info.values():
        for file_space_row in file_space_row_list:
            if file_space_row.volume is None:
                null_count += 1
                volume_name = "null-{0}".format(null_count)
            else:
                volume_name = file_space_row.volume

            volume_name_by_space_id[file_space_row.space_id] = volume_name

    return volume_name_by_space_id

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)


    zeromq_context = zmq.Context()

    resources = \
        _resources_tuple(halt_event=Event(),
                         volume_by_space_id=_volume_name_by_space_id(),
                         pull_socket=zeromq_context.socket(zmq.PULL),
                         router_socket=zeromq_context.socket(zmq.ROUTER),
                         event_push_client=\
                            EventPushClient(zeromq_context, 
                                            "rs_io_controller"),
                         pending_work_by_volume=defaultdict(deque),
                         available_ident_by_volume=defaultdict(deque))

    log.debug("binding to {0}".format(io_controller_pull_socket_uri))
    resources.pull_socket.bind(io_controller_pull_socket_uri)

    resources.router_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("binding to {0}".format(io_controller_router_socket_uri))
    resources.router_socket.bind(io_controller_router_socket_uri)

    # we poll the sockets for readability, we assume we can always
    # write to the router socket
    poller = zmq.Poller()
    poller.register(resources.pull_socket, zmq.POLLIN | zmq.POLLERR)
    poller.register(resources.router_socket, zmq.POLLIN| zmq.POLLERR)

    worker_processes = list()
    for volume_name in set(resources.volume_by_space_id.values()):
        for index in range(_worker_count):
            worker_processes.append(_launch_io_worker(volume_name, index+1))
    
    try:
        while not halt_event.is_set():
            for worker_process in worker_processes:
                poll_subprocess(worker_process)
            for active_socket, event_flags in poller.poll(_poll_timeout):
                if event_flags & zmq.POLLERR:
                    error_message = \
                        "error flags from zmq {0}".format(active_socket)
                    log.error(error_message)
                    raise PollError(error_message)
                if active_socket is resources.pull_socket:
                    _read_pull_socket(resources)
                elif active_socket is resources.router_socket:
                    _read_router_socket(resources)
                else:
                    log.error("unknown socket {0}".format(active_socket))
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error) and halt_event.is_set():
            log.info("program teminates normally with interrupted system call")
        else:
            log.exception("zeromq error processing request")
            resources.event_push_client.exception(unhandled_exception_topic,
                                                  "zeromq_error",
                                                  exctype="ZMQError")
            return_value = 1
    except Exception as instance:
        log.exception("error processing request")
        resources.event_push_client.exception(unhandled_exception_topic,
                                    str(instance),
                                    exctype=instance.__class__.__name__)
        return_value = 1
    else:
        log.info("program teminates normally")
    finally:
        for worker_process in worker_processes:
            terminate_subprocess(worker_process)
        resources.pull_socket.close()
        resources.router_socket.close()
        resources.event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

