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
import logging
import os
import sys

from diyapi_tools import message_driven_process as process

from messages.space_accounting_detail import SpaceAccountingDetail

_log_path = u"/var/log/pandora/diyapi_space_accounting_server_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "space-accounting-%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
_routing_header = "space-accounting"
_routing_key_binding = ".".join([_routing_header, "*"])

def _handle_detail(state, message_body):
    log = logging.getLogger("_handle_detail")
    message = SpaceAccountingDetail.unmarshall(message_body)
    log.info("avatar_id = %s, event = %s, value = %s" % (
        message.avatar_id, message.event, message.value
    ))

    return []

_dispatch_table = {
    SpaceAccountingDetail.routing_key   : _handle_detail,
}

if __name__ == "__main__":
    state = dict()
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state
        )
    )

