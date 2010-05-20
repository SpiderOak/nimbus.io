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
import datetime
import logging
import os
import sys
import time

from diyapi_tools import message_driven_process as process
from diyapi_tools.low_traffic_thread import LowTrafficThread, \
        low_traffic_routing_tag

from messages.space_accounting_detail import SpaceAccountingDetail

from diyapi_space_accounting_server.space_accounting_database import \
        SpaceAccountingDatabase

_log_path = u"/var/log/pandora/diyapi_space_accounting_server_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "space-accounting-%s" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"], 
)
_routing_header = "space-accounting"
_routing_key_binding = ".".join([_routing_header, "*"])
_low_traffic_routing_key = ".".join([
    _routing_header, 
    low_traffic_routing_tag,
])

def _create_state():
    return {"data" : dict()}

def _floor_hour(raw_datetime):
    """return a datetime rounded to the floor hour"""
    return datetime.datetime(
        year = raw_datetime.year,
        month = raw_datetime.month,
        day = raw_datetime.day,
        hour = raw_datetime.hour,
        minute = 0,
        second = 0,
        microsecond = 0
    )

def _next_dump_interval():
    """five minutes past the hour"""
    current_time = datetime.datetime.now()
    next_time = datetime.datetime(
        year = current_time.year,
        month = current_time.month,
        day = current_time.day,
        hour = current_time.hour,
        minute = 5,
        second = 0,
        microsecond = 0
    )
    if current_time.minute >= 5:
        next_time += datetime.timedelta(hours=1)
    return time.mktime(next_time.timetuple())

def _handle_low_traffic(_state, _message_body):
    log = logging.getLogger("_handle_low_traffic")
    log.debug("ignoring low traffic message")
    return None

def _handle_detail(state, message_body):
    log = logging.getLogger("_handle_detail")
    message = SpaceAccountingDetail.unmarshall(message_body)
    message_datetime = datetime.datetime.fromtimestamp(message.timestamp)
    message_hour = _floor_hour(message_datetime)
    log.info("hour = %s avatar_id = %s, event = %s, value = %s" % (
        message_hour, message.avatar_id, message.event, message.value
    ))

    hour_entry = state["data"].setdefault(message_hour, dict())
    avatar_entry = hour_entry.setdefault(message.avatar_id, dict())
    avatar_entry[message.event] = \
        avatar_entry.setdefault(message.event, 0) + message.value

    return []

_dispatch_table = {
    SpaceAccountingDetail.routing_key   : _handle_detail,
    _low_traffic_routing_key            : _handle_low_traffic,
}

def _startup(halt_event, state):
    state["low_traffic_thread"] = LowTrafficThread(
        halt_event, _routing_header
    )
    state["low_traffic_thread"].start()
    state["next_dump_interval"] = _next_dump_interval()

    return []

def _check_dump_time(state):
    """dump stats to database, if enough time has elapsed"""
    log = logging.getLogger("_check_dump_time")

    state["low_traffic_thread"].reset()

    if time.time() < state["next_dump_interval"]:
        return []

    state["next_dump_interval"] = _next_dump_interval()

    # we want to dump everything for the previous hour
    current_time = datetime.datetime.now()
    current_hour = _floor_hour(current_time)
    prev_hour = current_hour -  datetime.timedelta(hours=1)

    if prev_hour not in state["data"]:
        log.warn("no data for %s" % (prev_hour, ))
        return []

    log.info("storing data for %s" % (prev_hour, ))
    timestamp = time.mktime(prev_hour.timetuple())
    space_accounting_database = SpaceAccountingDatabase()
    for avatar_id, events in state["data"] [prev_hour]:
        space_accounting_database.store_avatar_stats(
            avatar_id,
            timestamp,
            events.get(SpaceAccountingDetail.bytes_added, 0),
            events.get(SpaceAccountingDetail.bytes_retrieved, 0),
            events.get(SpaceAccountingDetail.bytes_removed, 0)
        )
    space_accounting_database.commit()

    # clear out everything except the current hour
    for key in state["data"].keys():
        if key != current_hour:
            del state["data"][key]

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
            in_loop_function=_check_dump_time,
            post_loop_function=_shutdown
        )
    )

