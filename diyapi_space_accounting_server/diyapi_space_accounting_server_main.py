# -*- coding: utf-8 -*-
"""
diyapi_space_accounting_server.py

Runs on every server (for symmetry.)
Receives space accounting messages.
Accumulates diffs for each avatar in memory.
Diffs are grouped by hour. I.e added = [hournumber][avatar_id] = bytes added. 
Similar for bytes retrieved and bytes removed.
5 minutes into the next hour, the lowest numbered node dumps stats to the 
database, and announces to other nodes that it has done so.
Other nodes clear their memory of an hour's data when notified of a successful
db dump by any node.
If 10 minutes into hour the 2nd lowest node hasn't noticed a database dump, 
it makes its own dump. This repeats at 15 minutes for the 3rd lowest node, etc. 
Since there are twelve 5 minute segments in an hour, this works for ten nodes 
without a more complicated election process. :)
"""
from collections import deque
import datetime
import logging
import os
import sys
import time

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.xrep_server import XREPServer
from diyapi_tools.pull_server import PULLServer
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.data_definitions import parse_timestamp_repr

from diyapi_space_accounting_server.space_accounting_database import \
        SpaceAccountingDatabase, SpaceAccountingDatabaseAvatarNotFound
from diyapi_space_accounting_server.state_cleaner import StateCleaner
from diyapi_space_accounting_server.util import floor_hour

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_space_accounting_server_%s.log" % (
    _local_node_name,
)

_space_accounting_server_address = os.environ.get(
    "DIYAPI_SPACE_ACCOUNTING_SERVER_ADDRESS",
    "tcp://127.0.0.1:8300"
)

_space_accounting_pipeline_address = os.environ.get(
    "DIYAPI_SPACE_ACCOUNTING_PIPELINE_ADDRESS",
    "tcp://127.0.0.1:8350"
)

def _handle_space_accounting_detail(state, message, _data):
    log = logging.getLogger("_handle_space_accounting_detail")
    message_datetime = parse_timestamp_repr(message["timestamp-repr"])
    message_hour = floor_hour(message_datetime)
    log.info("hour = %s avatar_id = %s, event = %s, value = %s" % (
        message_hour, message["avatar-id"], message["event"], message["value"]
    ))

    hour_entry = state["data"].setdefault(message_hour, dict())
    avatar_entry = hour_entry.setdefault(message["avatar-id"], dict())
    avatar_entry[message["event"]] = \
        avatar_entry.setdefault(message["event"], 0) + message["value"]

def _handle_space_usage_request(state, message, _data):
    log = logging.getLogger("_handle_space_usage_request")
    log.info("request for avatar %s" % (message["avatar-id"],))

    reply = {
        "message-type"  : "space-usage-reply",
        "xrep-ident"    : message["xrep-ident"],
        "avatar-id"     : message["avatar-id"],
        "message-id"    : message["message-id"],
        "result"        : None,
    }

    # get sums of stats from the database
    space_accounting_database = SpaceAccountingDatabase(transaction=False)
    try:
        stats = space_accounting_database.retrieve_avatar_stats(
            message["avatar-id"]
        )
    except SpaceAccountingDatabaseAvatarNotFound, instance:
        error_message = "avatar not found %s" % (instance, )
        log.warn(error_message)
        reply["result"] = "unknown-avatar"
        reply["error-message"] = error_message
        state["xrep-server"].queue_message_for_send(reply)
        return
    finally:
        space_accounting_database.close()

    bytes_added, bytes_removed, bytes_retrieved = stats

    # increment sums with data from state
    for key in state["data"].keys():
        if message["avatar-id"] in state["data"][key]:
            events = state["data"][key][message["avatar-id"]]
            bytes_added += events.get("bytes_added", 0)
            bytes_removed += events.get("bytes_removed", 0)
            bytes_retrieved += events.get("bytes_retrieved", 0)
        
    reply["result"] = "success"
    reply["bytes-added"] = long(bytes_added)
    reply["bytes-removed"] = long(bytes_removed)
    reply["bytes-retrieved"] = long(bytes_retrieved)
    state["xrep-server"].queue_message_for_send(reply)

_dispatch_table = {
    "space-accounting-detail"   : _handle_space_accounting_detail,
    "space-usage-request"       : _handle_space_usage_request,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "pull-server"           : None,
        "xrep-server"           : None,
        "state-cleaner"         : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
        "data"                  : dict(),
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    log.info("binding xrep-server to %s" % (_space_accounting_server_address, ))
    state["xrep-server"] = XREPServer(
        state["zmq-context"],
        _space_accounting_server_address,
        state["receive-queue"]
    )
    state["xrep-server"].register(state["pollster"])

    log.info("binding pull-server to %s" % (
        _space_accounting_pipeline_address, 
    ))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _space_accounting_pipeline_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["state-cleaner"] = StateCleaner(state)

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["state-cleaner"].run, state["state-cleaner"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping xrep server")
    state["xrep-server"].close()

    log.debug("stopping pull server")
    state["pull-server"].close()

    state["zmq-context"].term()

    log.debug("teardown complete")

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

