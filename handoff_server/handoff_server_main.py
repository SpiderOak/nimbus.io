# -*- coding: utf-8 -*-
"""
handoff_server_main.py

"""
from collections import namedtuple, deque
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
from tools.data_definitions import segment_row_template, \
        conjoined_row_template, \
        create_priority
from tools.LRUCache import LRUCache

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
_conjoined_timestamps_template = namedtuple("ConjoinedTimestmaps", [
    "create_timestamp", 
    "abort_timestamp", 
    "complete_timestamp",
    "delete_timestamp"])
_retrieve_timeout = 30 * 60.0
_already_seen_cache_size = 1024 ** 2

def _retrieve_conjoined_handoffs_for_node(connection, node_id):
    result = connection.fetch_all_rows("""
        select %s from nimbusio_node.conjoined
        where handoff_node_id = %%s
        order by unified_id
    """ % (",".join(conjoined_row_template._fields), ), [node_id, ])

    if result is None:
        return None

    conjoined_row_list = list()
    for entry in result:
        row = conjoined_row_template._make(entry)

        # bytea columns come out of the database as buffer objects
        if row.combined_hash is None: 
            combined_hash = None
        else: 
            combined_hash = str(row.combined_hash)
        row = row._replace(combined_hash=combined_hash)
        conjoined_row_list.append(row)

    return conjoined_row_list

def _retrieve_segment_handoffs_for_node(connection, node_id):
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

def _fetch_conjoined_timestamps(connection, unified_id):
    raw_conjoined_timestamps = connection.fetch_one_row("""
        select create_timestamp, abort_timestamp, complete_timestamp,
        delete_timestamp from nimbusio_node.conjoined 
        where unified_id = %s""", [unified_id, ])

    if raw_conjoined_timestamps is None:
        return None

    return _conjoined_timestamps_template._make(raw_conjoined_timestamps)

def _insert_conjoined_row(connection, conjoined_dict):
    connection.execute("""
        insert into nimbusio_node.conjoined
        (collection_id, key, unified_id, create_timestamp, abort_timestamp,
         complete_timestamp, delete_timestamp)
        values (%(collection_id)s, %(key)s, %(unified_id)s, 
               %(create_timestamp)s, %(abort_timestamp)s,
               %(complete_timestamp)s, %(delete_timestamp)s)""", 
        conjoined_dict)

def _update_conjoined_row(connection, conjoined_timestamps, conjoined_dict):
    """
    We match the row we got from our database (conjoined_timestamps)
    against the row that came from the remote node (conjoined_dict).

    We assume a non None value overrides (is newer than) None
    """
    set_clauses = list()

    if conjoined_dict["create_timestamp"] is not None:
        if conjoined_timestamps.create_timestamp is None or \
           conjoined_dict["create_timestamp"] > \
           conjoined_timestamps.create_timestamp:
            set_clauses.append("create_timestamp = %(create_timestamp)s")
        
    if conjoined_dict["abort_timestamp"] is not None:
        if conjoined_timestamps.abort_timestamp is None or \
           conjoined_dict["abort_timestamp"] > \
           conjoined_timestamps.abort_timestamp:
            set_clauses.append("abort_timestamp = %(abort_timestamp)s")
        
    if conjoined_dict["complete_timestamp"] is not None:
        if conjoined_timestamps.complete_timestamp is None or \
           conjoined_dict["complete_timestamp"] > \
           conjoined_timestamps.complete_timestamp:
            set_clauses.append("complete_timestamp = %(complete_timestamp)s")
        
    if conjoined_dict["delete_timestamp"] is not None:
        if conjoined_timestamps.delete_timestamp is None or \
           conjoined_dict["delete_timestamp"] > \
           conjoined_timestamps.delete_timestamp:
            set_clauses.append("delete_timestamp = %(delete_timestamp)s")
        
    if len(set_clauses) == 0:
        return

    command_list = ["update nimbusio_node.conjoined set"]
    command_list.extend(set_clauses)
    command_list.append(
        "where unified_id = %(unified_id)s and handoff_node_id is null"
    )
    command = " ".join(command_list)
    connection.execute(command, conjoined_dict)

def _apply_conjoined_handoffs(connection, conjoined_dicts):
    log = logging.getLogger("_apply_conjoined_handoffs")

    handoff_node_id = None
    unified_ids = set()

    if len(conjoined_dicts) == 0:
        return handoff_node_id, list(unified_ids)

    # 2012-03-23 dougfort -- to begin with, let's just apply these 
    # conjoined handoffs as they come in, that may be enough
    
    connection.begin_transaction()
    try:
        for conjoined_dict in conjoined_dicts:
            if handoff_node_id is None:
                handoff_node_id = conjoined_dict["handoff_node_id"]
            assert conjoined_dict["handoff_node_id"] == handoff_node_id
            unified_ids.add(conjoined_dict["unified_id"])
            conjoined_timestamps = \
                    _fetch_conjoined_timestamps(connection,
                                                conjoined_dict["unified_id"])
            if conjoined_timestamps is None:
                _insert_conjoined_row(connection, conjoined_dict)
            else:
                _update_conjoined_row(connection, 
                                      conjoined_timestamps, 
                                      conjoined_dict)
    except Exception:
        connection.rollback()
        log.exception(str(conjoined_dict))
        raise
    connection.commit()

    return handoff_node_id, list(unified_ids)

def _handle_request_handoffs(state, message, _data):
    log = logging.getLogger("_handle_request_handoffs")
    log.debug("node %s %s" % (
        message["node-name"], 
        message["request-timestamp-repr"],
    ))

    reply = {
        "message-type"              : "request-handoffs-reply",
        "client-tag"                : message["client-tag"],
        "message-id"                : message["message-id"],
        "request-timestamp-repr"    : message["request-timestamp-repr"],
        "node-name"                 : _local_node_name,
        "conjoined-count"           : None,
        "segment-count"             : None,
        "result"                    : None,
        "error-message"             : None,
    }

    node_id = state["node-id-dict"][message["node-name"]]
    try:
        conjoined_rows = _retrieve_conjoined_handoffs_for_node(
            state["database-connection"], node_id
        )
        segment_rows = _retrieve_segment_handoffs_for_node(
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

    if conjoined_rows is None:
        conjoined_rows = []

    if segment_rows is None:
        segment_rows = []

    reply["conjoined-count"] = len(conjoined_rows)
    reply["segment-count"] = len(segment_rows)
    log.debug("found %s conjoined, %s segments" % (
        reply["conjoined-count"], reply["segment-count"], 
    ))

    data_dict = dict()
    # convert the rows from namedtuple through ordered_dict to regular dict
    data_dict["conjoined"] = \
            [dict(row._asdict().items()) for row in conjoined_rows]
    data_dict["segment"] = \
            [dict(row._asdict().items()) for row in segment_rows]
    data = pickle.dumps(data_dict)
        
    state["resilient-server"].send_reply(reply, data)

def _handle_request_handoffs_reply(state, message, data):
    log = logging.getLogger("_handle_request_handoffs_reply")
    log.debug("node %s %s conjoined-count=%s segment-count=%s %s" % (
        message["node-name"], 
        message["result"], 
        message["conjoined-count"], 
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
    if message["conjoined-count"] == 0 and message["segment-count"] == 0:
        return

    try:
        data_dict = pickle.loads(data)
    except Exception:
        log.exception("unable to load handoffs from %s" % (
            message["node-name"],
        ))
        return

    source_node_name = message["node-name"]

    segment_count  = 0
    already_seen_count = 0
    for entry in data_dict["segment"]:
        segment_row = segment_row_template(**entry)
        cache_key = (segment_row.id, source_node_name, )
        if cache_key in state["already-seen-cache"]:
            already_seen_count += 1
            continue
        state["already-seen-cache"]["cache_key"] = None
        state["pending-handoffs"].push(segment_row, source_node_name)
        segment_count += 1
    if already_seen_count > 0:
        log.info("ignored {0} handoff segments -= already seen".format(
            already_seen_count))
    if segment_count > 0:
        log.info("pushed {0} handoff segments".format(segment_count))

    handoff_node_id, unified_ids = \
            _apply_conjoined_handoffs(state["database-connection"], 
                                      data_dict["conjoined"])

    if len(unified_ids) > 0:
        # purge the handoff source(s)
        message = {
            "message-type"      : "purge-handoff-conjoined",
            "priority"          : create_priority(),
            "unified-ids"       : unified_ids,
            "handoff-node-id"   : handoff_node_id
        }
        writer_client = state["writer-client-dict"][source_node_name]
        writer_client.queue_message_for_send(message)
        log.info("{0} conjoined handoffs".format(len(unified_ids)))

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
            "message-type"      : "purge-handoff-segment",
            "priority"          : create_priority(),
            "unified-id"        : segment_row.unified_id,
            "conjoined-part"    : segment_row.conjoined_part,
            "handoff-node-id"   : segment_row.handoff_node_id
        }
        for source_node_name in source_node_names:
            writer_client = state["writer-client-dict"][source_node_name]
            writer_client.queue_message_for_send(message)

def _handle_destroy_key_reply(state, message, _data):
    log = logging.getLogger("_handle_destroy_key_reply")

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

    try:
        source_node_names = state["active-deletes"].pop(message["unified-id"])
    except KeyError:
        log.error("unknown reply %s" % (message["unified-id"], ))
        return

    # purge the handoff source(s)
    message = {
        "message-type"      : "purge-handoff-segment",
        "priority"          : create_priority(),
        "unified-id"        : message["unified-id"],
        "conjoined-part"    : 0,
        "handoff-node-id"   : state["node-id-dict"][_local_node_name],
    }
    for source_node_name in source_node_names:
        writer_client = state["writer-client-dict"][source_node_name]
        writer_client.queue_message_for_send(message)

def _handle_purge_handoff_conjoined_reply(_state, message, _data):
    log = logging.getLogger("_handle_purge_handoff_conjoined_reply")

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

def _handle_purge_handoff_segment_reply(_state, message, _data):
    log = logging.getLogger("_handle_purge_handoff_segment_reply")

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
    "destroy-key-reply"             : _handle_destroy_key_reply,
    "purge-handoff-conjoined-reply" : _handle_purge_handoff_conjoined_reply,
    "purge-handoff-segment-reply"   : _handle_purge_handoff_segment_reply,
}

def _create_state():
    return {
        "database-connection"       : None,
        "cluster-row"               : None,
        "node-rows"                 : None,
        # list of rows for each node in the cluster
        "node-id-dict"              : None,
        # maps node ids to names
        "node-name-dict"            : None,
        # maps node names to ids
        "zmq-context"               : zmq.Context(),
        "pollster"                  : ZeroMQPollster(),
        # used for other handoff servers to ask us for the handoffs that should
        # go to their node.  acknowledgements of the messages are sent
        # immediately, and then the received messages are put into
        # receive-queue (below) to be dispatched.
        "resilient-server"          : None,
        # general event notification system used throughout nimbus.io
        # we also push an event for every handoff that we initiate.
        "event-push-client"         : None,
        # this is the address that our resilient clients (to data readers, data
        # writers, and handoff servers) receive responses on. 
        # (resilient clients receive acknowledgements to the request directly
        # via REQ/REP, but the actual responses come in via this pull server.)
        # The pull server puts the messages it receives into receive-queue
        # (below) and they are then dispatched by queue-dipatcher (below.)
        # Note that resilient-server (above) also puts messages into that same
        # receive-queue.
        "pull-server"               : None,
        # maps node name to 10 data writer resilient clients
        "reader-client-dict"        : dict(),
        # maps node name to 10 data reader resilient clients
        # responses are sent to pull-server (above.)
        "writer-client-dict"        : dict(),
        # 9 resilient clients (connected to other nodes). 
        # responses are sent to pull-server (above.)
        "handoff-server-clients"    : list(),
        # messages received by our resilient server are put into this queue to
        # be dispatched.  So are messages recevied in response to all the
        # requests from our resilient clients
        "receive-queue"             : deque(),
        # this is the dispatcher object for messages in the above queue.
        # it uses the _dispatch_table defined above to process messages 
        # received via our resilient server AND by our pipeline address
        # (pull-server, above.)
        "queue-dispatcher"          : None,

        # a time queue that periodically sends request-handoffs messages
        # via each of handoff-server-clients (above).
        "handoff-requestor"         : None,
        # a time queue task peridically initiates the fulfillment of a handoff
        # held by another node for our node.
        "handoff-starter"           : None,
        # a data structure that holds all the pending handoffs we need to
        # retrieve from other nodes, in unified_id order and grouped by
        # unified_id (i.e. the same handoff from 2 nodes is stored together in
        # this structure.)
        "pending-handoffs"          : PendingHandoffs(),
        # a forwarder co-routine in use to transfer exactly one handoff to us
        # from another node.  A new forwarder object is created for each
        # handoff.  only one forwarder is active at any given time.
        "forwarder"                 : None,
        "already-seen-cache"        : LRUCache(_already_seen_cache_size),
        "active-deletes"            : dict(),
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

