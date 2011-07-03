# -*- coding: utf-8 -*-
"""
diyapi_handoff_server_main.py

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

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.resilient_server import ResilientServer
from diyapi_tools.resilient_client import ResilientClient
from diyapi_tools.pull_server import PULLServer
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.pandora_database_connection import get_node_local_connection
from diyapi_tools.data_definitions import parse_timestamp_repr, \
        segment_row_template

from diyapi_web_server.database_util import node_rows

from diyapi_handoff_server.handoff_requestor import HandoffRequestor, \
        handoff_polling_interval

class HandoffError(Exception):
    pass

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_handoff_server_%s.log" % (
    _local_node_name,
)
_data_reader_addresses = \
    os.environ["DIYAPI_DATA_READER_ADDRESSES"].split()
_data_writer_addresses = \
    os.environ["DIYAPI_DATA_WRITER_ADDRESSES"].split()
_client_tag = "handoff_server-%s" % (_local_node_name, )
_handoff_server_addresses = \
    os.environ["DIYAPI_HANDOFF_SERVER_ADDRESSES"].split()
_handoff_server_pipeline_address = os.environ.get(
    "DIYAPI_HANDOFF_SERVER_PIPELINE_ADDRESS",
    "tcp://127.0.0.1:8700"
)

_retrieve_timeout = 30 * 60.0


def _retrieve_handoffs_for_node(connection, node_id):
    result = connection.fetch_one_row("""
        select %s from diy.segment 
        where handoff_node_id = %%s
        order by timestamp desc
    """ % (",".join(segment_row_template._fields), ), [node_id, ])

    if result is None:
        return None

    return [segment_row_template._make(row) for row in result]

def _handle_request_handoffs(state, message, _data):
    log = logging.getLogger("_handle_request_handoffs")
    log.info("node %s id %s %s" % (
        message["node-name"], 
        message["node-id"],
        message["request-timestamp-repr"],
    ))

    reply = {
        "message-type"              : "request-handoffs",
        "client-tag"                : message["client-tag"],
        "message-id"                : message["message-id"],
        "request-timestamp-repr"    : message["request-timestamp-repr"],
        "node-name"                 : _local_node_name,
        "segment-count"             : None,
        "result"                    : None,
        "error-message"             : None,
    }


    try:
        rows = _retrieve_handoffs_for_node(
            state["database-connection"], message["node-id"]
        )
    except Exception, instance:
        log.exception(str(instance))
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
    data_list = [dict(row._asdict().items) for row in rows]
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

    rows = list()
    for entry in data_list:
        rows.append(
            segment_row_template(
                id=entry["id"],
                avatar_id=entry["avatar_id"],
                key=entry["key"],
                timestamp=entry["timestamp"],
                segment_num=entry["segment_num"],
                file_size=entry["file_size"],
                file_adler32=entry["file_adler32"],
                file_hash=entry["file_hash"],
                file_user_id=entry["file_user_id"],
                file_group_id=entry["file_group_id"],
                file_permissions=entry["file_permissions"],
                file_tombstone=entry["file_tombstone"],
                file_handoff_node_id=entry["handoff_node_id"]
            )
        )


def _handle_retrieve_reply(state, message, data):
    log = logging.getLogger("_handle_retrieve_reply")

    try:
        forwarder = state["active-forwarders"].pop(message["message-id"])
    except KeyError:
        log.error("no forwarder for message %s" % (message, ))
        return

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

    message_id = forwarder.send((message, data, ))
    assert message_id is not None
    state["active-forwarders"][message_id] = forwarder    

def _handle_archive_reply(state, message, _data):
    log = logging.getLogger("_handle_archive_reply")

    try:
        forwarder = state["active-forwarders"].pop(message["message-id"])
    except KeyError:
        log.error("no forwarder for message %s" % (message, ))
        return

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

    # if we get back a string, it is a message-id for another archive
    # otherwise, we should get the hint we started with
    result = forwarder.send(message)
    assert result is not None

    if type(result) is str:
        message_id = result
        state["active-forwarders"][message_id] = forwarder
    else:
        hint = result
        log.info("handoff complete %s %s %s" % (
            hint.node_name, hint.avatar_id, hint.key
        ))
        try:
            state["hint-repository"].purge_hint(hint)
        except Exception, instance:
            log.exception(instance)
        state["data-writer-status-checker"].check_node_for_hint(hint.node_name)

def _handle_purge_key_reply(state, message, _data):
    log = logging.getLogger("_handle_purge_key_reply")

    try:
        forwarder = state["active-forwarders"].pop(message["message-id"])
    except KeyError:
        log.error("no forwarder for message %s" % (message, ))
        return

    #TODO: we need to squawk about this somehow
    if message["result"] != "success":
        log.error("%s failed (%s) %s %s" % (
            message["message-type"], 
            message["result"], 
            message["error-message"], 
            message,
        ))
        # we don't give up here, because the handoff has succeeded 
        # at this point we're just cleaning up

    # if we get back a string, it is a message-id for another purge
    # otherwise, we should get the hint we started with
    result = forwarder.send(message)
    assert result is not None

    if type(result) is str:
        message_id = result
        state["active-forwarders"][message_id] = forwarder
    else:
        hint = result
        log.info("handoff complete %s %s %s" % (
            hint.node_name, hint.avatar_id, hint.key
        ))
        try:
            state["hint-repository"].purge_hint(hint)
        except Exception, instance:
            log.exception(instance)
        state["data-writer-status-checker"].check_node_for_hint(hint.node_name)

_dispatch_table = {
    "request-handoffs"              : _handle_request_handoffs,
    "request-handoffs-reply"        : _handle_request_handoffs_reply,
    "retrieve-key-start-reply"      : _handle_retrieve_reply,
    "retrieve-key-next-reply"       : _handle_retrieve_reply,
    "retrieve-key-final-reply"      : _handle_retrieve_reply,
    "archive-key-start-reply"       : _handle_archive_reply,
    "archive-key-next-reply"        : _handle_archive_reply,
    "archive-key-final-reply"       : _handle_archive_reply,
    "purge-key-reply"               : _handle_purge_key_reply,
}

def _create_state():
    return {
        "database-connection"       : None,
        "node-rows"                 : None,
        "node-id-dict"              : None,
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        "resilient-server"          : None,
        "pull-server"               : None,
        "reader-clients"            : list(),
        "writer-clients"            : list(),
        "handoff-server-clients"    : list(),
        "receive-queue"             : deque(),
        "queue-dispatcher"          : None,
        "handoff-requestor"         : None,
        "active-forwarders"         : dict(),
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")
    status_checkers = list()

    state["database-connection"] = get_node_local_connection()

    state["node-rows"] = node_rows(state["database-connection"])
    state["node-id-dict"] = dict(
        [(node_row.name, node_row.id, ) for node_row in state["node-rows"]]
    )

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
        state["reader-clients"].append(data_reader_client)
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
        state["writer-clients"].append(data_writer_client)
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

    timer_driven_callbacks = [
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
    for reader_client in state["reader-clients"]:
        reader_client.close()

    log.debug("closing writer clients")
    for writer_client in state["writer-clients"]:
        writer_client.close()

    log.debug("closing handoff_server clients")
    for handoff_server_client in state["handoff-server-clients"]:
        handoff_server_client.close()

    state["zmq-context"].term()

    state["database-connection"].close()

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ]
        )
    )

