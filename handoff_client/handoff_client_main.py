#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
handoff_client_main.py
"""
import logging
import os
import sys
from threading import Event

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.process_util import set_signal_handler

from handoff_client.options import parse_commandline
from handoff_client.get_node_ids import get_node_ids
from handoff_client.node_databases import get_node_databases
from handoff_client.get_handoff_rows import get_handoff_rows
from handoff_client.process_conjoined_rows import process_conjoined_rows
from handoff_client.process_segment_rows import process_segment_rows

_log_path = "{0}/nimbusio_handoff_client.log".format(
    os.environ["NIMBUSIO_LOG_DIR"])

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    args = parse_commandline()

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context =  zmq.Context()

    event_push_client = EventPushClient(zeromq_context, "handoff_client")
    event_push_client.info("program-start", "handoff_client starts")  

    return_code = 0
    node_databases = None
    try:
        node_dict = get_node_ids(args.node_name)
        node_databases = get_node_databases([args.node_name, ])
        conjoined_rows, segment_rows = \
            get_handoff_rows(node_databases, node_dict[args.node_name])
        log.info("found {0} conjoined and {1} segment handoffs".format(
            len(conjoined_rows), len(segment_rows)))
        if len(conjoined_rows)  > 0:
            process_conjoined_rows(halt_event, node_databases, conjoined_rows)
        if len(segment_rows)  > 0:
            process_segment_rows(halt_event, 
                                 zeromq_context, 
                                 args, 
                                 node_databases,
                                 segment_rows)
    except Exception as instance:
        log.exception("Uhandled exception {0}".format(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return_code = 1

    if node_databases is not None:
        for connection in node_databases.values():
            connection.close()
    event_push_client.close()
    zeromq_context.term()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())

