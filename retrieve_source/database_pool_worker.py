# -*- coding: utf-8 -*-
"""
database_pool_workerer.py

One of a pool of database workers
"""
import logging
import os
import os.path
import sys
from threading import Event

import psycopg2
import zmq

from tools.standard_logging import initialize_logging
from tools.zeromq_util import is_interrupted_system_call, \
        InterruptedSystemCall
from tools.process_util import set_signal_handler
from tools.event_push_client import EventPushClient, unhandled_exception_topic
from tools.database_connection import get_node_local_connection
from tools.data_definitions import segment_sequence_template

from retrieve_source.internal_sockets import db_controller_router_socket_uri

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path_template = "{0}/nimbusio_rs_db_pool_worker_{1}_{2}.log"
_all_sequence_rows_for_segment_query = """
select {0} 
from nimbusio_node.segment_sequence seq 
inner join nimbusio_node.value_file val
on seq.value_file_id = val.id
where seq.segment_id = (
    select id from nimbusio_node.segment 
    where unified_id = %(segment-unified-id)s
    and conjoined_part = %(segment-conjoined-part)s
    and segment_num = %(segment-num)s
    and handoff_node_id is null
    and status = 'F'
    limit 1
)
order by seq.sequence_num asc"""
_all_sequence_rows_for_handoff_query = """
select {0} 
from nimbusio_node.segment_sequence seq 
inner join nimbusio_node.value_file val
on seq.value_file_id = val.id
where seq.segment_id = (
    select id from nimbusio_node.segment 
    where unified_id = %(segment-unified-id)s
    and conjoined_part = %(segment-conjoined-part)s
    and segment_num = %(segment-num)s
    and handoff_node_id = %(handoff-node-id)s
    and status = 'F'
)
order by seq.sequence_num asc"""

def _send_initial_work_request(dealer_socket):
    """
    start the work cycle by notifying the controller that we are available
    """
    log = logging.getLogger("_send_initial_work_request")
    log.debug("sending initial request")
    message = {"message-type" : "ready-for-work"}
    dealer_socket.send_pyobj(message)

def _define_seq_val_fields():
    fields = ",".join(
        ["seq.{0}".format(f) for f in segment_sequence_template._fields])
    fields = ",".join([fields, "val.space_id"])
    return fields

def _process_one_transaction(dealer_socket, 
                             database_connection, 
                             event_push_client):
    """
    Wait for a reply to our last message from the controller.
    This will be a query request.
    We send the results to the controller and repeat the cycle, waiting for
    a reply
    """
    log = logging.getLogger("_process_one_transaction")
    log.debug("waiting work request")
    try:
        request = dealer_socket.recv_pyobj()
    except zmq.ZMQError as zmq_error:
        if is_interrupted_system_call(zmq_error):
            raise InterruptedSystemCall()
        raise
    assert dealer_socket.rcvmore
    control = dealer_socket.recv_pyobj()

    fields = _define_seq_val_fields()
    if request["handoff-node-id"] is None:
        query = _all_sequence_rows_for_segment_query.format(fields)
    else:
        query = _all_sequence_rows_for_handoff_query.format(fields)

    control["result"] = "success"
    control["error-message"] = ""
    try:
        result = database_connection.fetch_all_rows(query, request)    
    except psycopg2.OperationalError as instance:
        error_message = "database error {0}".format(instance)
        event_push_client.error("database_error", error_message)
        control["result"] = "database_error"
        control["error-message"] = error_message
    else:        
        if len(result) == 0:
            control["result"] = "no_sequence_rows_found"
            control["error-message"] = "no sequence rows found"

    if control["result"] != "success":
        log.error("{0} {1}".format(control["result"], 
                                   control["error-message"]))
        dealer_socket.send_pyobj(request, zmq.SNDMORE)
        dealer_socket.send_pyobj(control)
        return

    result_list = list()
    for row in result:
        row_list = list(row)
        segment_sequence_row = segment_sequence_template._make(row_list[:-1]) 
        space_id = row_list[-1]
        row_dict = dict(segment_sequence_row._asdict().items())
        row_dict["hash"] = bytes(row_dict["hash"])
        row_dict["space_id"] = space_id
        result_list.append(row_dict)

    log.debug("sending request back to controller")
    dealer_socket.send_pyobj(request, zmq.SNDMORE)
    dealer_socket.send_pyobj(control, zmq.SNDMORE)
    dealer_socket.send_pyobj(result_list)

def main():
    """
    main entry point
    returns 0 for normal termination (usually SIGTERM)
    """
    return_value = 0

    worker_number = int(sys.argv[1])

    log_path = _log_path_template.format(os.environ["NIMBUSIO_LOG_DIR"], 
                                         worker_number,
                                         _local_node_name)
    initialize_logging(log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    halt_event = Event()
    set_signal_handler(halt_event)

    zeromq_context = zmq.Context()

    event_source_name = "rs_dbpool_worker_{0}".format(worker_number)
    event_push_client = EventPushClient(zeromq_context, event_source_name)

    dealer_socket = zeromq_context.socket(zmq.DEALER)
    dealer_socket.setsockopt(zmq.LINGER, 1000)
    log.debug("connecting to {0}".format(db_controller_router_socket_uri))
    dealer_socket.connect(db_controller_router_socket_uri)

    log.debug("opening local database connection")
    database_connection = get_node_local_connection()

    try:
        _send_initial_work_request(dealer_socket)
        while not halt_event.is_set():
            _process_one_transaction(dealer_socket, 
                                     database_connection,
                                     event_push_client)
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
        database_connection.close()
        dealer_socket.close()
        event_push_client.close()
        zeromq_context.term()

    return return_value

if __name__ == "__main__":
    sys.exit(main())

