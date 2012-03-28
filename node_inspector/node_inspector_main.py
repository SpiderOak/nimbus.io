# -*- coding: utf-8 -*-
"""
garbage_collector_main.py
"""
import io
import logging
import os
import sys

import zmq

from tools.standard_logging import initialize_logging
from tools.database_connection import get_node_local_connection
from tools.event_push_client import EventPushClient

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = "{0}/nimbusio_node_inspector_{1}.log".format(
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)

def main():
    """
    main entry point
    return 0 for success (exit code)
    """
    initialize_logging(_log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    try:
        connection = get_node_local_connection()
    except Exception as instance:
        log.exception("Exception connecting to database {0}".format(instance))
        return -1

    zmq_context =  zmq.Context()

    event_push_client = EventPushClient(zmq_context, "node_inspector")
    event_push_client.info("program-start", "node_inspector starts")  

    return_code = 0


    connection.close()

    event_push_client.close()
    zmq_context.term()

    return return_code

if __name__ == "__main__":
    sys.exit(main())

