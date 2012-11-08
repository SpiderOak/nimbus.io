#!/usr/bin/env python3

# -*- coding: utf-8 -*-
"""
handoff_client_main.py
"""
import logging
import os
import sys

import zmq

from tools.standard_logging import initialize_logging
from tools.event_push_client import EventPushClient, unhandled_exception_topic

from handoff_client.get_node_ids import get_node_ids
from handoff_client.get_handoff_rows import get_handoff_rows

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_handoff_client_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,)

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "handoff_client")
    event_push_client.info("program-start", "handoff_client starts")  

    return_code = 0
    try:
        node_dict = get_node_ids(_local_node_name)
        conjoined_rows, segment_rows = \
            get_handoff_rows(node_dict[_local_node_name])
#        if len(conjoined_rows) + len(segment_rows) > 0:
#            _process_handoff_rows(conjoined_rows, segment_rows)
    except Exception as instance:
        log.exception("Uhandled exception {0}".format(instance))
        event_push_client.exception(
            unhandled_exception_topic,
            str(instance),
            exctype=instance.__class__.__name__
        )
        return_code = 1

    event_push_client.close()
    zmq_context.term()

    log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())

