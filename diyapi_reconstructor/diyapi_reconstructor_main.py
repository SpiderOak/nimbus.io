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
import datetime
import logging
import os
import random
import sys
import time
import uuid

from diyapi_tools import message_driven_process as process
from diyapi_tools.amqp_connection import local_exchange_name 
from diyapi_tools.low_traffic_thread import LowTrafficThread, \
        low_traffic_routing_tag

from messages.rebuild_request import RebuildRequest
from messages.rebuild_reply import RebuildReply
from messages.database_avatar_database_request import \
    DatabaseAvatarDatabaseRequest
from messages.database_avatar_database_reply import DatabaseAvatarDatabaseReply
from messages.process_status import ProcessStatus

_log_path = u"/var/log/pandora/diyapi_reconstructor_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "reconstructor-%s" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"], 
)
_routing_header = "reconstructor"
_routing_key_binding = ".".join([_routing_header, "*"])
_database_avatar_database_reply_routing_key = ".".join([
    _routing_header,
    DatabaseAvatarDatabaseReply.routing_tag,
])
_low_traffic_routing_key = ".".join([
    _routing_header, 
    low_traffic_routing_tag,
])
_request_timeout = 5.0 * 60.0
_exchanges = os.environ["DIY_NODE_EXCHANGES"].split()

_request_state_tuple = namedtuple("RequestState", [ 
    "timestamp",
    "timeout",
    "timeout_function",
    "avatar_id",
    "dest_dir",
    "replies",    
    "reply_exchange",
    "reply_routing_header"
])

def _is_request_state((_, value, )):
    return value.__class__.__name__ == "RequestState"

def _create_state():
    import socket
    return {
        "repository-path" : os.environ("DIYAPI_REPOSITORY_PATH"),
        "host"            : socket.gethostname(),
    }

def _timeout_request(request_id, state):
    """
    If we don't hear from all the nodes in a reasonable time,
    give up.
    """
    log = logging.getLogger("_timeout_request")
    try:
        request_state = state.pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return

def _handle_rebuild_request(state, message_body):
    """handle a request to rebuild the data for an avatar"""
    log = logging.getLogger("_handle_rebuild_request")
    message = RebuildRequest.unmarshall(message_body)
    log.info("request for rebuild of %s" % (message.avatar_id, )) 

    timestamp = datetime.datetime.now()
    
    state[message.request_id] = _request_state_tuple(
        timestamp=timestamp,
        timeout=time.time()+_request_timeout,
        timeout_function=_timeout_request,
        avatar_id=message.avatar_id,
        dest_dir=os.path.join(
            state["repository-path"], "rebuild", message.request_id
        ),
        replies=dict(), 
        reply_exchange=message.reply_exchange,
        reply_routing_header=message.reply_routing_header
    )
    os.makedirs(state[message.request_id].dest_dir)

    message = DatabaseAvatarDatabaseRequest(
        message.request_id,
        message.avatar_id,
        state["host"],
        state[message.request_id].dest_dir,
        local_exchange_name,
        _routing_header
    )
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

