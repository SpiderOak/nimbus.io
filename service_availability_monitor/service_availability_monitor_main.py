# -*- coding: utf-8 -*-
"""
service_availability_monitor_main.py

Service Availability Monitor
 - Spawns processes to monitor each service
 - PULLs results of each individual monitoring program
 - PUSHes events for changes in reachability of each service
"""
from collections import namedtuple
import logging
import os
import os.path
import subprocess
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        prepare_ipc_path, \
        ipc_socket_uri
from tools.process_util import identify_program_dir, \
        set_signal_handler, \
        poll_subprocess
from tools.event_push_client import EventPushClient, unhandled_exception_topic

_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_service_availability_monitor_{1}.log"
_socket_dir = os.environ["NIMBUSIO_SOCKET_DIR"]
_pull_socket_uri = ipc_socket_uri(_socket_dir, 
                                  _local_node_name,
                                  "service_availability_monitor")
_pull_socket_hwm = 1000
_poll_timeout = 3000 # milliseconds
_reporting_interval = 60.0

_ping_process_desc = namedtuple("PingProcessDesc", ["module_dir",
                                                    "file_name",
                                                    "service_name",
                                                    "ping_uris", ])

_ping_process = namedtuple("PingProcess", ["service_name",
                                           "node_name",
                                           "process", 
                                           "reachable_state", ])
_ping_process_descs = [ 
    _ping_process_desc(module_dir="zmq_ping",
                       file_name="zmq_ping_main.py",
                       service_name="retrieve_source",
                       ping_uris=os.environ["NIMBUSIO_DATA_READER_ADDRESSES"]),
    _ping_process_desc(module_dir="zmq_ping",
                       file_name="zmq_ping_main.py",
                       service_name="data_writer",
                       ping_uris=os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"]),
    _ping_process_desc(module_dir="zmq_ping",
                       file_name="zmq_ping_main.py",
                       service_name="handoff_server",
                       ping_uris=\
                           os.environ["NIMBUSIO_HANDOFF_SERVER_ADDRESSES"]),
    _ping_process_desc(module_dir="http_ping",
                       file_name="http_ping_main.py",
                       service_name="web_server",
                       ping_uris=os.environ["NIMBUSIO_WEB_SERVER_ADDRESSES"]),
    _ping_process_desc(module_dir="http_ping",
                       file_name="http_ping_main.py",
                       service_name="web_manager",
                       ping_uris=os.environ["NIMBUSIO_WEB_MANAGER_ADDRESSES"]),
]

def _bind_pull_socket(zeromq_context):
    log = logging.getLogger("_bind_pull_socket")

    pull_socket = zeromq_context.socket(zmq.PULL)
    pull_socket.setsockopt(zmq.HWM, _pull_socket_hwm)
    log.info("binding to {0} hwm = {1}".format(_pull_socket_uri,
                                               _pull_socket_hwm))
    pull_socket.bind(_pull_socket_uri)

    return pull_socket

def _launch_ping_process(ping_process_desc, node_name, ping_uri):
    log = logging.getLogger("launch_ping_process")
    module_dir = identify_program_dir(ping_process_desc.module_dir)
    module_path = os.path.join(module_dir, ping_process_desc.file_name)
    
    args = [sys.executable, module_path,
            "-u", ping_uri,
            "-r", _pull_socket_uri,
            "-m", str(_pull_socket_hwm), ]

    log.info("starting {0}".format(args))
    process = subprocess.Popen(args, bufsize=4096, stderr=subprocess.PIPE)

    return _ping_process(service_name=ping_process_desc.service_name,
                         node_name=node_name,
                         process=process,
                         reachable_state=None)

def _start_ping_processes(halt_event):
    log = logging.getLogger("_start_ping_processes")
    ping_process_dict = dict()
    for ping_process_desc in _ping_process_descs:
        ping_uris = ping_process_desc.ping_uris.split()
        for node_name, ping_uri in zip(_node_names, ping_uris):
            log.debug("launching {0} {1} {2}".format(
                ping_process_desc.service_name, node_name, ping_uri))
            ping_process_dict[ping_uri] = \
                _launch_ping_process(ping_process_desc, node_name, ping_uri)

            # don't slam all the subprocesses out in one lump
            halt_event.wait(1.0)
            if halt_event.is_set():
                break
        if halt_event.is_set():
            break

    return ping_process_dict

def _terminate_ping_processes(ping_process_dict):
    log = logging.getLogger("_terminate_ping_processes")
    for ping_process in ping_process_dict.values():
        log.debug("terminating {0} {1}".format(ping_process.service_name,
                                               ping_process.node_name))

        ping_process.process.poll()
        if ping_process.process.returncode is None:
            ping_process.process.terminate()

    # if some of these processes are still running, kill them
    for ping_process in ping_process_dict.values():
        ping_process.process.poll()
        if ping_process.process.returncode is None:
            log.debug("killing {0} {1}".format(ping_process.service_name,
                                               ping_process.node_name))

            ping_process.process.kill()

    for ping_process in ping_process_dict.values():
        ping_process.process.poll()
        if ping_process.process.returncode != 0:
            _, stderr_data = ping_process.process.communicate()
            if ping_process.process.returncode != 0:
                log.error("process ({0}) {1}".format(
                    ping_process.process.returncode, 
                    stderr_data.decode("utf-8")))

def _process_one_message(message, ping_process_dict, event_push_client):
    """
    process one ping message, report state change 
    """
    log = logging.getLogger("_process_one_message")
    assert message["message-type"] == "ping-result"

    reachable_state = message["result"] == "ok"
    ping_process = ping_process_dict[message["url"]]

    if reachable_state == ping_process.reachable_state:
        return
    
    description = \
        "{0} ping {1} from {2} state changes from {3} to {4} --- {5}".format(
        ping_process.service_name,
        ping_process.node_name,
        _local_node_name,
        ping_process.reachable_state,
        reachable_state,
        message["result"])
    log.info(description)

    event_push_client.info("service-availability-state-change",
                           description,
                           service_name=ping_process.service_name, 
                           local_node_name=_local_node_name,
                           target_node_name=ping_process.node_name,
                           check_number=message["check-number"], 
                           socket_reconnection_number=\
                               message["socket-reconnection-number"], 
                           reachable=reachable_state)

    ping_process_dict[message["url"]] = \
        ping_process._replace(reachable_state=reachable_state)

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

    prepare_ipc_path(_pull_socket_uri)

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    pull_socket = _bind_pull_socket(zeromq_context)

    event_push_client = EventPushClient(zeromq_context, "service_availability")
    event_push_client.info("program-starts", 
                           "service availability monitor starts")

    message_count = 0
    try:
        ping_process_dict = _start_ping_processes(halt_event)

        while not halt_event.is_set():

            if message_count % len(ping_process_dict) == 0:
                for ping_process in ping_process_dict.values():
                    poll_subprocess(ping_process.process)

            message = pull_socket.recv_pyobj()
            assert not pull_socket.rcvmore

            _process_one_message(message, ping_process_dict, event_push_client)

            message_count += 1

    except KeyboardInterrupt: # convenience for testing
        log.info("keyboard interrupt: terminating normally")
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error) and halt_event.is_set():
            log.info("program terminating normally; interrupted system call")
        else:
            log.exception("zeromq error processing request")
            event_push_client.exception(unhandled_exception_topic,
                                        "zeromq_error",
                                        exctype="ZMQError")
            return_value = 1
    except Exception as instance:
        log.exception("error processing request")
        event_push_client.exception(unhandled_exception_topic,
                                    str(instance),
                                    exctype=instance.__class__.__name__)
        return_value = 1
    else:
        log.info("program teminating normally")

    log.debug("terminating subprocesses")
    _terminate_ping_processes(ping_process_dict)
    pull_socket.close()
    event_push_client.close()
    zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

