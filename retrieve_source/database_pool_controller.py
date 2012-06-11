# -*- coding: utf-8 -*-
"""
database_pool_controller.py

Manage a pool of database workers
"""
from collections import deque
import logging
import os
import os.path
import pickle
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall
from tools.process_util import identify_program_dir, \
        set_signal_handler, \
        poll_subprocess, \
        terminate_subprocess
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from retrieve_source.internal_sockets import db_controller_pull_socket_uri, \
        db_controller_router_socket_uri

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_db_pool_controller_{1}.log"
_worker_count = int(os.environ.get("NIMBUSIO_RETRIEVE_DB_POOL_COUNT", "2"))
_poll_timeout = 3.0 

def _launch_database_pool_worker(worker_number):
    log = logging.getLogger("launch_database_pool_worker")
    module_dir = identify_program_dir("retrieve_source")
    module_path = os.path.join(module_dir, "database_pool_worker.py")
    
    args = [sys.executable, module_path, str(worker_number), ]

    log.info("starting {0}".format(args))
    return subprocess.Popen(args, stderr=subprocess.PIPE)

def _send_pending_work_to_available_workers(pending_work_queue, 
                                            available_worker_ident_queue,
                                            router_socket):
    """
    send messages from the pending_work_queue 
    to workers in the available_worker_ident_queue_queue
    """
    log = logging.getLogger("_send_pending_work_to_available_workers")
    work_count = min(len(pending_work_queue), 
                     len(available_worker_ident_queue))
    for _ in range(work_count):
        message = pending_work_queue.popleft()
        ident = available_worker_ident_queue.popleft()
        router_socket.send(ident, zmq.SNDMORE)
        router_socket.send_json(message)

def _handle_retrieve_key_start(message, active_retrieves, pending_work_queue):
    log = logging.getLogger("_handle_retrieve_key_start")
    retrieve_id = message["retrieve-id"]
    if retrieve_id in active_retrieves:
        log.error("duplicate retrieve-id {0} in archive-key-start".format(
             retrieve_id))
        del active_retrieves[retrieve_id]

    log.debug("adding {0} to pending work queue".format(message))
    pending_work_queue.append(message)

def _handle_retrieve_key_next(message, active_retrieves, pending_work_queue):
    log = logging.getLogger("_handle_retrieve_key_next")
    retrieve_id = message["retrieve-id"]

    if retrieve_id not in active_retrieves:
        log.error("unknown retrieve-id {0} in archive-key-next".format(
             retrieve_id))
        return

_dispatch_table = { "retrieve-key-start" : _handle_retrieve_key_start,
                    "retrieve-key-next"  : _handle_retrieve_key_next, }

def _read_pull_socket(pull_socket, 
                      active_retrieves, 
                      pending_work_queue, 
                      available_worker_ident_queue,
                      router_socket):
    """
    read messages from the PULL socket until we would block
    if the message-type is 'retrieve-key-start'
        add to the pending_work queue

    if the message-type is 'retrieve-key-next'
        get the next segment_slice info from active_retrieves
        add the information to the request
        push the request to the disk_io_controller 
    """
    log = logging.getLogger("_read_pull_socket")

    while True: # read until we would block
        try:
            message = pull_socket.recv_json(zmq.NOBLOCK)
        except zmq.ZMQError as instance:
            if instance.errno == zmq.EAGAIN:
                break
            raise

        assert not pull_socket.rcvmore

        try:
            _dispatch_table[message["message-type"]](message,
                                                     active_retrieves,
                                                     pending_work_queue)
        except KeyError as instance:
            log.error("unknown message type {0} {1}".format(instance,
                                                            message))

    _send_pending_work_to_available_workers(pending_work_queue, 
                                            available_worker_ident_queue,
                                            router_socket)

def _read_router_socket(router_socket, 
                        active_retrieves,
                        pending_work_queue,
                        available_worker_ident_queue):
    """
    read a message from the router socket (from one of our worker processes)
    if the message-type is 'ready-for-work' (initial message)
        add the message ident to the available_worker_ident_queue
    if the message-type is 'work-complete'
        use the attached data to start an active_retrieve
        add the message ident to the available_worker_ident_queue
    """
    log = logging.getLogger("_read_router_socket")

    ident = router_socket.recv()
    assert router_socket.rcvmore
    message = router_socket.recv_json()
    data = None
    if router_socket.rcvmore:
        pickled_data = router_socket.recv()
        data = pickle.loads(pickled_data)

    #TODO: start active retrieve
    if message["message-type"] == "archive-key-start":
        active_retrieves[message["retrieve-id"]] = (message, data, )

    available_worker_ident_queue.append(ident) 
    _send_pending_work_to_available_workers(pending_work_queue, 
                                            available_worker_ident_queue,
                                            router_socket)

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

    pull_socket = zeromq_context.socket(zmq.PULL)
    log.debug("binding to {0}".format(db_controller_pull_socket_uri))
    pull_socket.bind(db_controller_pull_socket_uri)

    router_socket = zeromq_context.socket(zmq.ROUTER)
    router_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("binding to {0}".format(db_controller_router_socket_uri))
    router_socket.bind(db_controller_router_socket_uri)

    # we poll the sockets for readability, we assume we can always
    # write to the router socket
    poller = zmq.Poller()
    poller.register(pull_socket, zmq.POLLIN | zmq.POLLERR)
    poller.register(router_socket, zmq.POLLIN| zmq.POLLERR)

    event_push_client = EventPushClient(zeromq_context, 
                                        "rs_db_pool_controller")

    worker_processes = list()
    for index in range(_worker_count):
        worker_processes.append(_launch_database_pool_worker(index+1))
    
    # database results used for retrieves that are in progress
    active_retrieves = dict()

    # new messages from the pull socket which have not been passed to a
    # worker subprocess
    pending_work_queue = deque()

    # the message ident(s) from workers waiting for work
    available_worker_ident_queue = deque()

    try:
        while not halt_event.is_set():
            for worker_process in worker_processes:
                poll_subprocess(worker_process)
            for active_socket, event_flags in poller.poll(_poll_timeout):
                if event_flags & zmq.POLLERR:
                    log.error("error flags from zmq {0}".format(active_socket))
                    continue
                if active_socket == pull_socket:
                    _read_pull_socket(pull_socket, 
                                      active_retrieves, 
                                      pending_work_queue, 
                                      available_worker_ident_queue,
                                      router_socket)
                elif active_socket == router_socket:
                    _read_router_socket(router_socket, 
                                        active_retrieves, 
                                        pending_work_queue,
                                        available_worker_ident_queue)
                else:
                    log.error("unknown socket {0}".format(active_socket))
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
        pull_socket.close()
        for worker_process in worker_processes:
            terminate_subprocess(worker_process)
        router_socket.close()
        event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

