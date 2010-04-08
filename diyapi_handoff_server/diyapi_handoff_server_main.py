# -*- coding: utf-8 -*-
"""
diyapi_handoff_server_main.py

"""
import logging
import os
import sys
import time

from diyapi_tools import amqp_connection
from diyapi_tools import message_driven_process as process
from diyapi_tools.standard_logging import format_timestamp 

from messages.process_status import ProcessStatus

from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply

from diyapi_handoff_server import handoff_repository

_log_path = u"/var/log/pandora/diyapi_handoff_server_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "handoff-server-%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
_routing_header = "handoff_server"
_routing_key_binding = ".".join([_routing_header, "*"])

def _handle_hinted_handoff(state, message_body):
    log = logging.getLogger("_handle_hinted_handoff")
    message = HintedHandoff.unmarshall(message_body)
    log.info("avatar_id %s, key %s, version_number %s, segment_number %s" % (
        message.avatar_id, 
        message.key,  
        message.version_number, 
        message.segment_number
    ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", HintedHandoffReply.routing_tag]
    )

    try:
        handoff_repository.store(
            message.dest_exchange,
            message.timestamp,
            message.key,
            message.version_number,
            message.segment_number
        )
    except Exception, instance:
        log.exception(str(instance))
        reply = HintedHandoffReply(
            message.request_id,
            HintedHandoffReply.error_exception,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = HintedHandoffReply( 
        message.request_id, HintedHandoffReply.successful
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_process_status(state, message_body):
    log = logging.getLogger("_handle_process_status")
    message = ProcessStatus.unmarshall(message_body)
    log.debug("%s %s %s %s" % (
        message.exchange,
        message.routing_header,
        message.status,
        format_timestamp(message.timestamp),
    ))
    return []

_dispatch_table = {
    HintedHandoff.routing_key       : _handle_hinted_handoff,
    ProcessStatus.routing_key       : _handle_process_status,
}

def _startup(_halt_event, state):
    message = ProcessStatus(
        time.time(),
        amqp_connection.local_exchange_name,
        _routing_header,
        ProcessStatus.status_startup
    )

    exchange = amqp_connection.broadcast_exchange_name
    routing_key = ProcessStatus.routing_key

    return [(exchange, routing_key, message, )]

def _shutdown(state):
    message = ProcessStatus(
        time.time(),
        amqp_connection.local_exchange_name,
        _routing_header,
        ProcessStatus.status_shutdown
    )

    exchange = amqp_connection.broadcast_exchange_name
    routing_key = ProcessStatus.routing_key

    return [(exchange, routing_key, message, )]

if __name__ == "__main__":
    state = dict()
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_startup,
            in_loop_function=None,
            post_loop_function=_shutdown
        )
    )

