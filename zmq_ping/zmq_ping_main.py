# -*- coding: utf-8 -*-
"""
zmq_ping_main.py

The ZMQ check works like this:
 - Every 3s send a ping msg. Timeout after 5s.
 - Publish the result of successful pings and timeouts.
 - Every time a timeout happens, close and re-open the socket. Like the pirate.
 - Along with every status message, send a sequential number, 
   and the number of times we've reconnected to the socket.
"""
import argparse
import logging
import sys
from threading import Event

import zmq

from tools.standard_logging import _log_format_template
from tools.process_util import set_signal_handler

class CommandLineError(Exception):
    pass

_program_description = """
    send a ping message to a server and report the result
""".strip()

def _initialize_logging_to_stderr():
    """
    we log to stderr because we assume we are being called by a process
    that pipes standard error
    """
    log_level = logging.WARN
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _parse_command_line():
    """
    parse commandline
    return argument values
    """
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-u", "--url", dest="ping_url", 
                        help="The 0mq URL to ping")
    parser.add_argument("-r", "--reporting-url", dest="reporting_url", 
                        help="The 0mq URL to report results to")
    parser.add_argument("-m", "--hwm", dest="hwm", type=int, default=1000,
                        help="high water mark on reporting socket")
    parser.add_argument("-i", "--interval", dest="interval", 
                        type=int, default=3, 
                        help="seconds between ping attempts")
    parser.add_argument("-t", "--timeout", dest="timeout", 
                        type=int, default=5, 
                        help="seconds to wait for a reply to ping")

    args = parser.parse_args()

    if args.ping_url is None:
        raise CommandLineError("You must specify a URL to ping")

    if args.reporting_url is None:
        raise CommandLineError("You must specify a URL to report results to")

    return args

def main():
    """
    main entry point
    """
    args = _parse_command_line()
    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    reporting_socket = zeromq_context.socket(zmq.PUSH)
    reporting_socket.setsockopt(zmq.LINGER, 5000)
    reporting_socket.setsockopt(zmq.HWM, args.hwm)
    reporting_socket.connect(args.reporting_url)

    req_socket = None

    ping_message = {"message-type"   : "ping"}

    result_message = {"message-type"    : "ping-result",
                      "url"             : args.url,
                      "result"          : None,
                      "req-open-count"  : 0,
                      "ping-count"      : 0, }

    poller = zmq.Poller()

    while not halt_event.is_set():
        if req_socket is None:
            req_socket = zeromq_context.socket(zmq.REQ)
            req_socket.connect(args.url)
            result_message["req-open-count"] += 1
            poller.register(req_socket, zmq.POLLIN | zmq.POLLERR)

        result_message["ping-count"] += 1
        req_socket.send_json(ping_message)
        result = poller.poll(timeout=args.timeout)

        if len(result) == 0:
            result_message["result"] = "timeout"
        else:
            _, event_flags = result
            if event_flags & zmq.POLLERR:
                result_message["result"] = "socket-error"
            else:
                result_message["result"] = "ok"

        reporting_socket.send_pyobject(result_message)
    
        if result_message["result"] != "ok":
            poller.unregister(req_socket)
            req_socket.close()
            req_socket = None

        halt_event.wait(args.interval)
        
    if req_socket is not None:
        req_socket.close()
    reporting_socket.close()
    zeromq_context.term()

    return 0

if __name__ == "__main__":
    _initialize_logging_to_stderr()

    try:
        exit_code = main()
    except Exception as instance:
        logging.exception(str(instance))
        exit_code = 1

    sys.exit(exit_code)
    
