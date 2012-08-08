# -*- coding: utf-8 -*-
"""
test_web_internal_reader.py

"""
import argparse
import errno
import logging
import os
import socket
import sys
import urllib.error
import urllib.request

from tools.standard_logging import _log_format_template

class CommandLineError(Exception):
    pass

_web_internal_reader_host = \
    os.environ["NIMBUSIO_WEB_INTERNAL_READER_HOST"]
_web_internal_reader_port = \
    int(os.environ["NIMBUSIO_WEB_INTERNAL_READER_PORT"])

_program_description = """
    retrieve a file from the web_internal_reader, 
    given unitied-id and conjoined-part
""".strip()

def _initialize_logging_to_stderr():
    """
    we log to stderr
    """
    log_level = logging.DEBUG
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
    parser.add_argument("-u", "--unified-id", dest="unified_id", type=int,
                        help="The unified_id to retrieve")
    parser.add_argument("-c", "--conjoined-part", dest="conjoined_part", 
                        type=int, help="The conjoined part to retrieve")

    args = parser.parse_args()

    if args.unified_id is None:
        raise CommandLineError("You must specify a unified_id to retrieve")

    if args.conjoined_part is None:
        raise CommandLineError("You must specify a conjoined part to retrieve")

    return args

def _retrieve(args):
    log = logging.getLogger("_retrieve")
    address = ":".join([_web_internal_reader_host, 
                        str(_web_internal_reader_port)])

    url = "/".join(["http:/", 
                    address, 
                    "data", 
                    str(args.unified_id), 
                    str(args.conjoined_part)])
    log.info("retrieving {0}".format(url))

    try:
        with urllib.request.urlopen(url, timeout=60.0) as r:
            r.read()
    except urllib.error.URLError as url_error:
        if type(url_error) == socket.error:
            log.error("URLError socket.error {0} {1}".format(
                url_error.errno, url_error.strerror))
        else:
            log.error("URLError {0}".format(url_error.reason))
    except urllib.error.HTTPError as http_error:
        log.error("HTTP code {0}".format(http_error.code))
    except socket.error as socket_error:
        # URLLib does not catch all socket errors
        log.error("socket.error {0} {1}".format(socket_error.errno, 
                                                socket_error.strerror))
    else:
        log.info("no errors in retrieve")

def main():
    """
    main entry point
    """
    return_code = 0
    args = _parse_command_line()

    try:
        _retrieve(args)
    except Exception as instance:
        logging.exception("fatal exception {0}".format(str(instance)))
        return_code = 1

    return return_code

if __name__ == "__main__":
    _initialize_logging_to_stderr()

    try:
        exit_code = main()
    except Exception as instance:
        logging.exception(str(instance))
        exit_code = 1

    sys.exit(exit_code)
    
