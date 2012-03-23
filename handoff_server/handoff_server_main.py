# -*- coding: utf-8 -*-
"""
handoff_server_main.py

"""
from collections import deque
import logging
import os
import os.path
import cPickle as pickle
import random
import sys
import time

import zmq

from tools.zeromq_pollster import ZeroMQPollster
from tools.resilient_server import ResilientServer
from tools.event_push_client import EventPushClient, exception_event
from tools.resilient_client import ResilientClient
from tools.pull_server import PULLServer
from tools.deque_dispatcher import DequeDispatcher
from tools import time_queue_driven_process
from tools.database_connection import \
        get_node_local_connection, \
        get_central_connection
from tools.data_definitions import segment_row_template, create_priority

from web_server.central_database_util import get_cluster_row, \
        get_node_rows

from handoff_server.pending_handoffs import PendingHandoffs
from handoff_server.handoff_requestor import HandoffRequestor, \
        handoff_polling_interval
from handoff_server.handoff_starter import HandoffStarter

class HandoffError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_log_path = u"%s/nimbusio_handoff_server_%s.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], _local_node_name,
)
_data_reader_addresses = \
    os.environ["NIMBUSIO_DATA_READER_ADDRESSES"].split()
_data_writer_addresses = \
    os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"].split()
_client_tag = "handoff_server-%s" % (_local_node_name, )
_handoff_server_addresses = \
    os.environ["NIMBUSIO_HANDOFF_SERVER_ADDRESSES"].split()
_handoff_server_pipeline_address = os.environ.get(
    "NIMBUSIO_HANDOFF_SERVER_PIPELINE_ADDRESS",
    "tcp://127.0.0.1:8700"
)

_retrieve_timeout = 30 * 60.0


def _retrieve_handoffs_for_node(connection, node_id):
    result = connection.fetch_all_rows("""
        select %s from nimbusio_node.segment 
        where handoff_node_id = %%s
        order by timestamp desc
    """ % (",".join(segment_row_template._fields), ), [node_id, ])

    if result is None:
        return None

    segment_row_list = list()
    for entry in result:
        row = segment_row_template._make(entry)

        # bytea columns come out of the database as buffer objects
        if row.file_hash is None: 
            file_hash = None
        else: 
            file_hash = str(row.file_hash)
        row = row._replace(file_hash=file_hash)
        segment_row_list.append(row)

    return segment_row_list

def _convert_dict_to_segment_row(segment_dict):
    return segment_row_template(
        id=segment_dict["id"],
        collection_id=segment_dict["collection_id"],
        key=segment_dict["key"],
        status=segment_dict["status"],
        unified_id=segment_dict["unified_id"],
        timestamp=segment_dict["timestamp"],
        segment_num=segment_dict["segment_num"],
        conjoined_part=segment_dict.get("conjoined_part"),
        file_size=segment_dict["file_size"],
        file_adler32=segment_dict["file_adler32"],
        file_hash=segment_dict["file_hash"],
        file_tombstone_unified_id=segment_dict["file_tombstone_unified_id"],
        source_node_id=segment_dict["source_node_id"],
        handoff_node_id=segment_dict["handoff_node_id"]
    )

def _handle_request_handoffs(state, message, _data):
    log = logging.getLogger("_handle_request_handoffs")
    log.info("node %s %s" % (
        message["node-name"], 
        message["request-timestamp-repr"],
    ))

    reply = {
        "message-type"              : "request-handoffs-reply",
        "client-tag"                : message["client-tag"],
        "message-id"                : message["message-id"],
        "request-timestamp-repr"    : message["request-timestamp-repr"],
        "node-name"                 : _local_node_name,
        "segment-count"             : None,
        "result"                    : None,
        "error-message"             : None,
    }

    node_id = state["node-id-dict"][message["node-name"]]
    try:
        rows = _retrieve_handoffs_for_node(
            state["database-connection"], node_id
        )
    except Exception, instance:
        log.exception(str(instance))
        state["event-push-client"].exception(
            "_retrieve_handoffs_for_node", str(instance)
        )  
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    reply["result"] = "success"

    if rows is None:
        reply["segment-count"] = 0
        state["resilient-server"].send_reply(reply)
        return

    reply["segment-count"] = len(rows)
    log.debug("found %s segments" % (reply["segment-count"], ))

    # convert the rows from namedtuple through ordered_dict to regular dict
    data_list = [dict(row._asdict().items()) for row in rows]
    data = pickle.dumps(data_list)
        
    state["resilient-server"].send_reply(reply, data)

def _handle_request_handoffs_reply(state, message, data):
    log = logging.getLogger("_handle_request_handoffs_reply")
    log.info("node %s %s segment-count %s %s" % (
        message["node-name"], 
        message["result"], 
        message["segment-count"], 
        message["request-timestamp-repr"],
    ))

    if message["result"] != "success":
        log.error("request-handoffs failed on node %s %s %s" % (
            message["node-name"], 
            message["result"], 
            message["error-message"],
        ))
        return

    # the normal case
    if message["segment-count"] == 0:
        return

    try:
        data_list = pickle.loads(data)
    except Exception:
        log.exception("unable to load handoffs from %s" % (
            message["node-name"],
        ))
        return

    source_node_name = message["node-name"]
    for entry in data_list:
        segment_row = _convert_dict_to_segment_row(entry)
        state["pending-handoffs"].push(segment_row, source_node_name)

def _handle_retrieve_key_reply(state, message, data):
    log = logging.getLogger("_handle_retrieve_key_reply")

    # 2012-02-05 dougfort -- handle a race condition where we pick up a segment
    # to be handed off after we have purged it, because the message was 
    # in transit
    if message["result"] == "no-sequence-rows":
        log.debug("no-sequence-rows, assuming already purged {0}".format(
            message
        ))
        state["forwarder"] = None
        return

    if message["result"] != "success":
        error_message = "%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        )
        log.error(error_message)
        raise HandoffError(error_message)

    state["forwarder"].send((message, data, ))

def _handle_archive_reply(state, message, _data):
    log = logging.getLogger("_handle_archive_reply")

    #TODO: we need to squawk about this somehow
    if message["result"] != "success":
        error_message = "%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        )
        log.error(error_message)
        raise HandoffError(error_message)

    result = state["forwarder"].send(message)

    if result is not None:
        state["forwarder"] = None

        segment_row, source_node_names = result

        description = "handoff complete %s %s %s %s" % (
            segment_row.collection_id,
            segment_row.key,
            segment_row.timestamp,
            segment_row.segment_num,
        )
        log.info(description)

        state["event-push-client"].info(
            "handoff-complete",
            description,
            backup_sources=source_node_names,
            collection_id=segment_row.collection_id,
            key=segment_row.key,
            timestamp_repr=repr(segment_row.timestamp)
        )
        
        # purge the handoff source(s)
        message = {
            "message-type"      : "purge-handoff-source",
            "priority"          : create_priority(),
            "unified-id"        : segment_row.unified_id,
            "conjoined-part"    : segment_row.conjoined_part,
            "handoff-node-id"   : segment_row.handoff_node_id
        }
        for source_node_name in source_node_names:
            writer_client = state["writer-client-dict"][source_node_name]
            writer_client.queue_message_for_send(message)

def _handle_purge_key_reply(_state, message, _data):
    log = logging.getLogger("_handle_purge_key_reply")

    #TODO: we need to squawk about this somehow
    if message["result"] == "success":
        log.debug("purge-key successful")
    else:
        log.error("%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        ))
        # we don't give up here, because the handoff has succeeded 
        # at this point we're just cleaning up

_dispatch_table = {
    "request-handoffs"              : _handle_request_handoffs,
    "request-handoffs-reply"        : _handle_request_handoffs_reply,
    "retrieve-key-reply"            : _handle_retrieve_key_reply,
    "archive-key-start-reply"       : _handle_archive_reply,
    "archive-key-next-reply"        : _handle_archive_reply,
    "archive-key-final-reply"       : _handle_archive_reply,
    "purge-key-reply"               : _handle_purge_key_reply,
}

def _create_state():
    return {
        "database-connection"       : None,
        "cluster-row"               : None,
        "node-rows"                 : None,
        "node-id-dict"              : None,
        "node-name-dict"            : None,
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "resilient-server"          : None,
        "event-push-client"         : None,
        "pull-server"               : None,
        "reader-client-dict"        : dict(),
        "writer-client-dict"        : dict(),
        "handoff-server-clients"    : list(),
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
        "handoff-requestor"         : None,
        "pending-handoffs"          : PendingHandoffs(),
        "forwarder"                 : None,
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")
    status_checkers = list()

    # do the event push client first, because we may need to
    # push an execption event from setup
    state["event-push-client"] = EventPushClient(
        state["zmq-context"],
        "handoff_server"
    )

    central_connection = get_central_connection()
    state["cluster-row"] = get_cluster_row(central_connection)
    state["node-rows"] = get_node_rows(
        central_connection, state["cluster-row"].id
    )
    central_connection.close()

    state["node-id-dict"] = dict(
        [(node_row.name, node_row.id, ) for node_row in state["node-rows"]]
    )
    state["node-name-dict"] = dict(
        [(node_row.id, node_row.name, ) for node_row in state["node-rows"]]
    )

    state["database-connection"] = get_node_local_connection()
    for node_row, handoff_server_address in zip(
        state["node-rows"], _handoff_server_addresses
    ):
        if node_row.name == _local_node_name:
            log.info("binding resilient-server to %s" % (
                handoff_server_address, 
            ))
            state["resilient-server"] = ResilientServer(
                state["zmq-context"],
                handoff_server_address,
                state["receive-queue"]
            )
            state["resilient-server"].register(state["pollster"])
        else:
            handoff_server_client = ResilientClient(
                state["zmq-context"],
                state["pollster"],
                node_row.name,
                handoff_server_address,
                _client_tag,
                _handoff_server_pipeline_address
            )
            state["handoff-server-clients"].append(handoff_server_client)
            # don't run all the status checkers at the same time
            status_checkers.append(
                (handoff_server_client.run, 
                 time.time() + random.random() * 60.0, )
            )        

    log.info("binding pull-server to %s" % (_handoff_server_pipeline_address, ))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _handoff_server_pipeline_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    for node_row, data_reader_address in zip(
        state["node-rows"], _data_reader_addresses
    ):
        data_reader_client = ResilientClient(
            state["zmq-context"],
            state["pollster"],
            node_row.name,
            data_reader_address,
            _client_tag,
            _handoff_server_pipeline_address
        )
        state["reader-client-dict"][data_reader_client.server_node_name] = \
                data_reader_client
        # don't run all the status checkers at the same time
        status_checkers.append(
            (data_reader_client.run, time.time() + random.random() * 60.0, )
        )        

    for node_row, data_writer_address in zip(
        state["node-rows"], _data_writer_addresses
    ):
        data_writer_client = ResilientClient(
            state["zmq-context"],
            state["pollster"],
            node_row.name,
            data_writer_address,
            _client_tag,
            _handoff_server_pipeline_address
        )
        state["writer-client-dict"][data_writer_client.server_node_name] = \
                data_writer_client
        # don't run all the status checkers at the same time
        status_checkers.append(
            (data_writer_client.run, time.time() + random.random() * 60.0, )
        )        

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["handoff-requestor"] = HandoffRequestor(state, _local_node_name)
    state["handoff-starter"] = HandoffStarter(
        state, _local_node_name, state["event-push-client"]
    )

    state["event-push-client"].info("program-start", "handoff_server starts")  

    timer_driven_callbacks = [
        (state["handoff-starter"].run, state["handoff-starter"].next_run(), ),
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        # try to spread out handoff polling, if all nodes start together
        (state["handoff-requestor"].run,
            time.time() + random.random() * handoff_polling_interval)
    ] 
    timer_driven_callbacks.extend(status_checkers)
    return timer_driven_callbacks

def _tear_down(state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping resilient server")
    state["resilient-server"].close()

    log.debug("stopping pull server")
    state["pull-server"].close()

    log.debug("closing reader clients")
    for reader_client in state["reader-client-dict"].values():
        reader_client.close()

    log.debug("closing writer clients")
    for writer_client in state["writer-client-dict"].values():
        writer_client.close()

    log.debug("closing handoff_server clients")
    for handoff_server_client in state["handoff-server-clients"]:
        handoff_server_client.close()

    state["event-push-client"].close()

    state["zmq-context"].term()

    state["database-connection"].close()

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ],
            exception_action=exception_event
        )
    )

