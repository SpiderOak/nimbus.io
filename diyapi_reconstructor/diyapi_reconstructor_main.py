# -*- coding: utf-8 -*-
"""
diyapi_reconstructor.py

Item level comparisons: 
    Request all 10 databases from database_servers. 
    Iterate through them. (since they are all sorted, 
    this doesn't require unbounded memory.) 
    Ignore keys stored in the last hour, which may still be settling. 
    Based on the timestamp values present for each key, 
    you should be able to determine the "correct" state. 
    I.e. if a tombstone is present, 
    it means any earlier keys should not be there. 
    If only some (but not all) shares are there, the remaining shares 
    should be reconstructed and added. 
    Any other situation would indicate a data integrity error 
    that should be resolved. 
    In addition to the entry in the table, if there are 1 or more errors, 
    let's keep a .gz'd text file of keys having an error.

Reconstructor Item Level Comparison Cases

All entries have the same timestamp and point to the same data. 
Most common case. No disagreement.
All entries have the same timestamp and point to the same tombstone. 
No disagreement.
No tombstones, all entries have the same timestamp and point to the same data, 
but fewer than 10 nodes have an entry. Disagreement. Reconstruct via ZFEC 
the data and add to nodes that do not have it 
(with the retroactive timestamp.) 
If reconstruction is not possible, log data loss.
Only tombstones, but not all nodes have the same tombstone 
or perhaps any tombstone at all. Disagreement. 
Send newest tombstone to all nodes that have no tombstone or an older tombstone 
(with the retroactive timestamp.)
A mixture of tombstones and data entries, with the newest being a tombstone. 
Disagreement. Send the tombstone to nodes that do not have it, 
or that have an older tombstone (with the retroactive timestamp.)
A mixture of data entries and tombstones, with a data entry being newest. 
This one has a couple of branches (described below.)
If there are sufficient ZFEC shares for the data described by the item 
with the newest timestamp, reconstruct and insert as in 3 above.

Otherwise, see if there are enough ZFEC shares to reconstruct the data with 
any previous timestamp (i.e. roll back to a previous state for this key.) 
If so, log data loss and the rollback, and insert the previous state
as in 3 above.

Otherwise log general data loss for the key.

"""
from collections import namedtuple
import logging
import os
import sys
import time

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.xrep_server import XREPServer
from diyapi_tools.pull_server import PULLServer
from diyapi_tools.resilient_client import ResilientClient
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.data_definitions import create_timestamp

_node_names = os.environ['SPIDEROAK_MULTI_NODE_NAME_SEQ'].split()
_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_reconstructor_%s.log" % (
    _local_node_name,
)

_client_tag = "reconstructor-%s" % (_local_node_name, )
_database_server_addresses = \
    os.environ["DIYAPI_DATABASE_SERVER_ADDRESSES"].split()
_reconstructor_address = os.environ.get(
    "DIYAPI_RECONSTRUCTOR_ADDRESS",
    "tcp://127.0.0.1:8800"
)
_reconstructor_pipeline_address = os.environ.get(
    "DIYAPI_RECONSTRUCTOR_PIPELINE_ADDRESS",
    "tcp://127.0.0.1:8850"
)
_request_timeout = 5.0 * 60.0
_request_state_tuple = namedtuple("RequestState", [ 
    "xrep_ident",
    "timestamp",
    "timeout",
    "timeout_function",
    "dest_dir",
    "replies",    
])

def _is_request_state((_, value, )):
    return value.__class__.__name__ == "RequestState"

def _create_state():
    import socket
    return {
        "repository-path"   : os.environ["DIYAPI_REPOSITORY_PATH"],
        "host"              : socket.gethostname(),
        "active-requests"   : dict()
    }

def _timeout_request(avatar_id, state):
    """
    If we don't hear from all the nodes in a reasonable time,
    give up.
    """
    log = logging.getLogger("_timeout_request")
    try:
        request_state = state.pop(avatar_id)
    except KeyError:
        log.error("can't find %s in state" % (avatar_id, ))
        return

    log.error("timeout on rebuild %s" % (
        avatar_id,
    ))

def _handle_rebuild_request(state, message, _data):
    """handle a request to rebuild the data for an avatar"""
    log = logging.getLogger("_handle_rebuild_request")
    log.info("request for rebuild of %s" % (message["avatar-id"], )) 

    if message["avatar-id"] in state["active-requests"]
        error_message = "rebuild already in progress for %s" % (
            message["avatar-id"]
        )
        log.error(error_message)
        reply = {
            "message-type"  : "rebuild-reply",
            "xrep-ident"    : message["xrep_ident"],
            "avatar-id"     : message["avatar-id"],
            "result"        : "duplicate",
            "error-message" : "rebuild already in progress for %s" % (
                message["avatar-id"]
            ),
        }
        state["xrep-server"].queue_message_for_send(reply)

    timestamp = create_timestamp()
    
    state["active-requests"][message["avatar-id"]] = _request_state_tuple(
        xrep_ident=,essage["xrep_ident"],
        timestamp=timestamp,
        timeout=time.time()+_request_timeout,
        timeout_function=_timeout_request,
        dest_dir=os.path.join(
            state["repository-path"], "rebuild", message["avatar-id"]
        ),
        replies=dict(), 
    )
    os.makedirs(state["active-requests"][message["avatar-id"]] .dest_dir)

    message = {
        message.request_id,
        message["avatar-id"],
        state["host"],
        state[message.request_id].dest_dir,
        local_exchange_name,
        _routing_header
    }
    # send the DatabaseAvatarDatabaseRequest to every node
    return [
        (dest_exchange, message.routing_key, message) \
        for dest_exchange in _exchanges
    ]

def _handle_low_traffic(_state, _message_body):
    log = logging.getLogger("_handle_low_traffic")
    log.debug("ignoring low traffic message")
    return None

def _handle_database_avatar_database_reply(state, message_body):
    log = logging.getLogger("_handle_database_avatar_database_reply")
    message = DatabaseAvatarDatabaseReply.unmarshall(message_body)

    if not message.request_id in state:
        # if we don't have any state for this request, 
        # all we can do is heave it
        log.warn("no state for request %s" % (message.request_id, ))
        return []

    request_state = state[message.request_id]

    reply_routing_key = ".".join([
        request_state.reply_routing_header,
        RebuildReply.routing_tag
    ])
    reply_exchange = request_state.reply_exchange

    # if we already have a reply for this node, something is badly wrong
    if message.node_name in request_state.replies:
        del state[message.request_id]
        error_message = "duplicate reply from node %s" % (message.node_name, )
        log.error(error_message)
        reply = RebuildReply(
            message.request_id,
            RebuildReply.other_error,
            error_message=error_message
        )
        return [(reply_exchange, reply_routing_key, reply, ), ]

    request_state.replies[message.node_name] = message.result

    if message.error:
        log.error("error %s from node %s %s" % (
            message.result, message.node_name, message.error_message
        ))

    # if we don't replies from all exchanges, wait for some more
    if len(request_state.replies) != len(_exchanges):
        return []

    # at this point we should have a reply from every node, so
    # we don't want to preserve state anymore
    del state[message.request_id]

    # To begin with, we're going to give up if we got any errors fetching
    # databases. Maybe later we could rebuild one or two missing databases
    if any(request_state.replies.values()):
        error_message = "errors fetching databases"
        log.error(error_message)
        reply = RebuildReply(
            message.request_id,
            RebuildReply.database_error,
            error_message=error_message
        )
        return [(reply_exchange, reply_routing_key, reply, ), ]

    return []

def _handle_process_status(_state, _message_body):
    """silently discard ProcessStatus to keep from cluttering up the log"""
    return []

_dispatch_table = {
    RebuildRequest.routing_key : _handle_rebuild_request,    
    _database_avatar_database_reply_routing_key   : \
        _handle_database_avatar_database_reply,
    ProcessStatus.routing_key           : _handle_process_status,
    _low_traffic_routing_key            : _handle_low_traffic,
}

def _startup(halt_event, state):
    state["low_traffic_thread"] = LowTrafficThread(
        halt_event, 
        _routing_header
    )
    state["low_traffic_thread"].start()
    return []

def _check_time(state):
    """check if enough time has elapsed"""
    log = logging.getLogger("_check_time")

    state["low_traffic_thread"].reset()

    current_time = time.time()

    # see if we have any timeouts
    for request_id, request_state in filter(_is_request_state, state.items()):
        if current_time > request_state.timeout:
            log.warn(
                "%s timed out waiting message; running timeout function" % (
                    request_id
                )
            )
            request_state.timeout_function(request_id, state)

    return []

def _shutdown(state):
    state["low_traffic_thread"].join()
    del state["low_traffic_thread"]
    return []

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_startup,
            in_loop_function=_check_time,
            post_loop_function=_shutdown
        )
    )

