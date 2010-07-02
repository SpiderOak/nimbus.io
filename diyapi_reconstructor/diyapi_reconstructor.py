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
If so, log data loss and the rollback, and insert the previous state as in 3 above.

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

from messages.reconstruct_request import ReconstructRequest
from messages.reconstruct_reply import ReconstructReply
from messages.database_avatar_database_request import \
    DatabaseAvatarDatabaseRequest
from messages.database_avatar_database_reply import DatabaseAvatarDatabaseReply

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
    "replies",
    "row_id",
    "reply_exchange",
    "reply_routing_header"
])

def _is_request_state((_, value, )):
    return value.__class__.__name__ == "RequestState"

def _create_state():
    return dict()

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

    database.close()

def _handle_reconstruct_request(state, message_body):
    """handle a requst to reconstruct the data for and avatar"""
    log = logging.getLogger("_handle_reconstruct_request")
    message = ReconstructRequest.unmarshall(message_body)
    log.info("request for reconsruct of %s" % (message.avatar_id, )) 

    timestamp = datetime.datetime.now()

    state[message.request_id] = _request_state_tuple(
        timestamp=timestamp,
        timeout=time.time()+_request_timeout,
        timeout_function=_timeout_request,
        avatar_id=message.avatar_id,
        replies=dict(), 
        row_id=message.row_id,
        reply_exchange=message.reply_exchange,
        reply_routing_header=message.reply_routing_header
    )

    message = DatabaseAvatarDatabaseRequest(
        message.request_id,
        message.avatar_id,
        time.mktime(timestamp.timetuple()),
        local_exchange_name,
        _routing_header
    )
    # send the DatabaseConsistencyCheck to every node
    return [
        (dest_exchange, message.routing_key, message) \
        for dest_exchange in _exchanges
    ]

def _handle_low_traffic(_state, _message_body):
    log = logging.getLogger("_handle_low_traffic")
    log.debug("ignoring low traffic message")
    return None

def _handle_database_avatar_list_reply(state, message_body):
    log = logging.getLogger("_handle_database_avatar_list_reply")
    message = DatabaseAvatarListReply.unmarshall(message_body)

    state["avatar-ids"] = set(message.get())
    log.info("found %s avatar ids" % (len(state["avatar-ids"]), ))

    # if we don't have a consistency check in progress, start one
    if not any(filter(_is_request_state, state.items())):
        return _choose_avatar_for_consistency_check(state)

    return []

def _handle_database_consistency_check_reply(state, message_body):
    log = logging.getLogger("_handle_database_consistency_check_reply")
    message = DatabaseConsistencyCheckReply.unmarshall(message_body)

    if not message.request_id in state:
        log.warn("Unknown request_id %s from %s" % (
            message.request_id, message.node_name
        ))
        return []

    request_id = message.request_id
    if message.error:
        log.error("%s (%s) %s from %s %s" % (
            state[request_id].avatar_id, 
            message.result,
            message.error_message,
            message.node_name,
            message.request_id
        ))
        hash_value = _error_hash
    else:
        hash_value = message.hash

    # if this audit was started by an AntiEntropyAuditRequest message,
    # we want to send a reply
    if state[request_id].reply_routing_header is not None:
        reply_routing_key = ".".join([
            state[request_id].reply_routing_header,
            AntiEntropyAuditReply.routing_tag
        ])
        reply_exchange = state[request_id].reply_exchange
        assert reply_exchange is not None
    else:
        reply_routing_key = None
        reply_exchange = None
        
    if message.node_name in state[request_id].replies:
        error_message = "duplicate reply from %s %s %s" % (
            message.node_name,
            state[request_id].avatar_id, 
            request_id
        )
        log.error(error_message)
        if reply_exchange is not None:
            reply_message = AntiEntropyAuditReply(
                request_id,
                AntiEntropyAuditReply.other_error,
                error_message
            )
            return [(reply_exchange, reply_routing_key, reply_message, ), ]
        else:
            return []

    state[request_id].replies[message.node_name] = hash_value

    # not done yet, wait for more replies
    if len(state[request_id].replies) < len(_exchanges):
        return []

    # at this point we should have a reply from every node, so
    # we don't want to preserve state anymore
    request_state = state.pop(request_id)
    database = AuditResultDatabase()
    timestamp = datetime.datetime.now()
    
    hash_list = list(set(request_state.replies.values()))
    
    # ok - all have the same hash
    if len(hash_list) == 1 and hash_list[0] != _error_hash:
        log.info("avatar %s compares ok" % (request_state.avatar_id, ))
        database.successful_audit(request_state.row_id, timestamp)
        if reply_exchange is not None:
            reply_message = AntiEntropyAuditReply(
                request_id,
                AntiEntropyAuditReply.successful
            )
            return [(reply_exchange, reply_routing_key, reply_message, ), ]
        else:
            return []

    # we have error(s), but the non-errors compare ok
    if len(hash_list) == 2 and _error_hash in hash_list:
        error_count = 0
        for value in request_state.replies.values():
            if value == _error_hash:
                error_count += 1

        # if we come from AntiEntropyAuditRequest, don't retry
        if reply_exchange is not None:
            database.audit_error(request_state.row_id, timestamp)
            database.close()
            error_message = "There were %s error hashes" % (error_count, )
            log.error(error_message)
            reply_message = AntiEntropyAuditReply(
                request_id,
                AntiEntropyAuditReply.other_error,
                error_message
            )
            return [(reply_exchange, reply_routing_key, reply_message, ), ]
        
        if request_state.retry_count >= _max_retry_count:
            log.error("avatar %s %s errors, too many retries" % (
                request_state.avatar_id, 
                error_count
            ))
            database.audit_error(request_state.row_id, timestamp)
            # TODO: needto do something here
        else:
            log.warn("avatar %s %s errors, will retry" % (
                request_state.avatar_id, 
                error_count
            ))
            state["retry-list"].append(
                _retry_entry_tuple(
                    retry_time=_retry_time(), 
                    avatar_id=request_state.avatar_id,
                    row_id=request_state.row_id,
                    retry_count=request_state.retry_count, 
                )
            )
            database.wait_for_retry(request_state.row_id)
        database.close()
        return []

    # if we make it here, we have some form of mismatch, possibly mixed with
    # errors
    error_message = "avatar %s hash mismatch" % (request_state.avatar_id, )
    log.error(error_message)
    for node_name, value in request_state.replies.items():
        log.error("    node %s value %s" % (node_name, value, ))

    # if we come from AntiEntropyAuditRequest, don't retry
    if reply_exchange is not None:
        database.audit_error(request_state.row_id, timestamp)
        database.close()
        reply_message = AntiEntropyAuditReply(
            request_id,
            AntiEntropyAuditReply.audit_error,
            error_message
        )
        return [(reply_exchange, reply_routing_key, reply_message, ), ]

    if request_state.retry_count >= _max_retry_count:
        log.error("%s too many retries" % (request_state.avatar_id, ))
        database.audit_error(request_state.row_id, timestamp)
        # TODO: need to do something here
    else:
        state["retry-list"].append(
            _retry_entry_tuple(
                retry_time=_retry_time(), 
                avatar_id=request_state.avatar_id,
                row_id=request_state.row_id,
                retry_count=request_state.retry_count, 
            )
        )
        database.wait_for_retry(request_state.row_id)

    database.close()
    return []

def _handle_process_status(_state, _message_body):
    """silently discard ProcessStatus to keep from cluttering up the log"""
    return []

_dispatch_table = {
    AntiEntropyAuditRequest.routing_key : \
        _handle_anti_entropy_audit_request,    
    _database_avatar_list_reply_routing_key   : \
        _handle_database_avatar_list_reply,
    _database_consistency_check_reply_routing_key   : \
        _handle_database_consistency_check_reply,
    ProcessStatus.routing_key           : _handle_process_status,
    _low_traffic_routing_key            : _handle_low_traffic,
}

def _startup(halt_event, state):
    state["low_traffic_thread"] = LowTrafficThread(
        halt_event, 
        _routing_header
    )
    state["low_traffic_thread"].start()
    state["next_poll_interval"] = _next_poll_interval()
    state["next_avatar_poll_interval"] = _next_avatar_poll_interval()
    return _request_avatar_ids()

def _check_time(state):
    """check if enough time has elapsed"""
    log = logging.getLogger("_check_time")

    state["low_traffic_thread"].reset()

    current_time = time.time()

    # see if we have any retries ready
    next_retry_list = list()
    for retry_entry in state["retry-list"]:
        if current_time >= retry_entry.retry_timestamp:
            _start_consistency_check(
                state,
                retry_entry.avatar_id, 
                row_id=retry_entry.row_id,
                retry_count=retry_entry.retry_count +1)
        else:
            next_retry_list.append(retry_entry)
    state["retry-list"] = next_retry_list

    # see if we have any timeouts
    for request_id, request_state in filter(_is_request_state, state.items()):
        if current_time > request_state.timeout:
            log.warn(
                "%s timed out waiting message; running timeout function" % (
                    request_id
                )
            )
            request_state.timeout_function(request_id, state)

    # periodically send DatabaseAvatarListRequest
    if current_time >= state["next_avatar_poll_interval"]:
        state["next_avatar_poll_interval"] = _next_avatar_poll_interval()
        return _request_avatar_ids()

    if current_time >= state["next_poll_interval"]:
        state["next_poll_interval"] = _next_poll_interval()
        return _choose_avatar_for_consistency_check(state)

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

