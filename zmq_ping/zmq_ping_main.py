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
import uuid

import zmq

from tools.standard_logging import _log_format_template
from tools.process_util import set_signal_handler
from tools.zeromq_util import is_interrupted_system_call

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

def _process_one_ping(halt_event,
                      args,
                      zeromq_context,
                      ping_message, 
                      result_message, 
                      req_socket, 
                      reporting_socket):

    if req_socket is None:
        req_socket = zeromq_context.socket(zmq.REQ)
        req_socket.setsockopt(zmq.RCVTIMEO, args.timeout * 1000)
        req_socket.connect(args.ping_url)
        result_message["socket-reconnection-number"] += 1

    result_message["check-number"] += 1
    req_socket.send_json(ping_message)

    try:
        _ = req_socket.recv_json()
    except zmq.ZMQError as instance:
        if instance.errno == zmq.EAGAIN:
            if halt_event.is_set():
                result_message["result"] = "ok"
            else:
                result_message["result"] = "timeout"
        else:
            result_message["result"] = "socket-error"
    else:
        result_message["result"] = "ok"

    if not halt_event.is_set():
        reporting_socket.send_pyobj(result_message)

        if result_message["result"] != "ok":
            req_socket.close()
            req_socket = None

    return req_socket


def main():
    """
    main entry point
    """
    return_code = 0
    args = _parse_command_line()
    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    reporting_socket = zeromq_context.socket(zmq.PUSH)
    reporting_socket.setsockopt(zmq.LINGER, 5000)
    reporting_socket.setsockopt(zmq.HWM, args.hwm)
    reporting_socket.connect(args.reporting_url)

    req_socket = None

    ping_message = {"message-type"   : "ping",
                    "message-id"     : uuid.uuid1().hex, }

    result_message = {"message-type"    : "ping-result",
                      "url"             : args.ping_url,
                      "result"          : None,
                      "socket-reconnection-number"  : 0,
                      "check-number"    : 0, }

    while not halt_event.is_set():
        try:
            req_socket = _process_one_ping(halt_event,
                                           args,
                                           zeromq_context,
                                           ping_message, 
                                           result_message, 
                                           req_socket, 
                                           reporting_socket)
         
        except zmq.ZMQError as zmq_error:
            if is_interrupted_system_call(zmq_error) and halt_event.is_set():
                pass
            else:
                logging.exception(str(zmq_error))
                halt_event.set()
                return_code = 1

        except Exception as instance:
            logging.exception(str(instance))
            halt_event.set()
            return_code = 1

        else:
            halt_event.wait(args.interval)
        
    if req_socket is not None:
        req_socket.close()
    reporting_socket.close()
    zeromq_context.term()

    return return_code

if __name__ == "__main__":
    _initialize_logging_to_stderr()

    try:
        exit_code = main()
    except Exception as instance:
        logging.exception(str(instance))
        exit_code = 1

    sys.exit(exit_code)
    
