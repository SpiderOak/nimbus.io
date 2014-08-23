# -*- coding: utf-8 -*-
"""
http_ping_main.py

The ZMQ check works like this:
 - Every 3s send a ping msg. Timeout after 10s.
 - Publish the result of successful pings and timeouts.
 - Along with every status message, send a sequential number, 
"""
import argparse
import errno
import logging
import socket
import sys
from threading import Event
import urllib.error
import urllib.request

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
                        help="The URL to ping")
    parser.add_argument("-r", "--reporting-url", dest="reporting_url", 
                        help="The 0mq URL to report results to")
    parser.add_argument("-m", "--hwm", dest="hwm", type=int, default=1000,
                        help="high water mark on reporting socket")
    parser.add_argument("-i", "--interval", dest="interval", 
                        type=int, default=3, 
                        help="seconds between ping attempts")
    parser.add_argument("-t", "--timeout", dest="timeout", 
                        type=int, default=10, 
                        help="seconds to wait for a reply to ping")

    args = parser.parse_args()

    if args.ping_url is None:
        raise CommandLineError("You must specify a URL to ping")

    if args.reporting_url is None:
        raise CommandLineError("You must specify a URL to report results to")

    return args

def _process_one_ping(halt_event,
                      args,
                      result_message, 
                      reporting_socket):

    result_message["check-number"] += 1
    url = "/".join([args.ping_url, "ping"])

    try:
        with urllib.request.urlopen(url, timeout=args.timeout) as r:
            r.read()
    except urllib.error.URLError as url_error:
        if type(url_error) == socket.error:
            result_message["result"] = "URLError socket.error {0} {1}".format(
                url_error.errno, url_error.strerror)
        else:
            result_message["result"] = "URLError {0}".format(url_error.reason)
    except urllib.error.HTTPError as http_error:
        result_message["result"] = "HTTP code {0}".format(http_error.code)
    except socket.error as socket_error:
        # URLLib does not catch all socket errors
        result_message["result"] = "socket.error {0} {1}".format(
            socket_error.errno, socket_error.strerror)
    else:
        result_message["result"] = "ok"

    if not halt_event.is_set():
        reporting_socket.send_pyobj(result_message)

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
    reporting_socket.setsockopt(zmq.SNDHWM, args.hwm)
    reporting_socket.connect(args.reporting_url)

    result_message = {"message-type"    : "ping-result",
                      "url"             : args.ping_url,
                      "result"          : None,
                      "socket-reconnection-number"  : 0,
                      "check-number"    : 0, }

    while not halt_event.is_set():
        try:
            _process_one_ping(halt_event,
                              args,
                              result_message, 
                              reporting_socket)
         
        except Exception as instance:
            logging.exception("fatal exception {0}".format(str(instance)))
            halt_event.set()
            return_code = 1

        else:
            halt_event.wait(args.interval)
        
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
    
