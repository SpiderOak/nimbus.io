# -*- coding: utf-8 -*-
"""
request_audit.py

request anti entroy audit of an avatar
arguments <avatar-id> <anti-entropy-exchange>
"""
import logging
import sys
import uuid

from diyapi_tools import amqp_connection
from diyapi_tools import message_driven_process as process

from messages.anti_entropy_audit_request import AntiEntropyAuditRequest
from messages.anti_entropy_audit_reply import AntiEntropyAuditReply

_log_path = u"/var/log/pandora/request_audit.log"

_queue_name = "request_audit"
_routing_key_binding = "request_audit.*"
_reply_routing_header = "request_audit"
anti_entropy_audit_reply_routing_key = ".".join([
    _reply_routing_header, AntiEntropyAuditReply.routing_tag
])

def _pre_loop_function(_halt_event, state):
    log = logging.getLogger("_pre_loop_function")

    avatar_id = int(sys.argv[1])
    anti_entropy_exchange = sys.argv[2]

    info = "requesting audit of %s at %s" % (avatar_id, anti_entropy_exchange, )
    log.info(info)
    print >> sys.stderr, ""
    print >> sys.stderr, info

    state["request-id"] = uuid.uuid1().hex
    local_exchange = amqp_connection.local_exchange_name

    message = AntiEntropyAuditRequest(
        state["request-id"],
        avatar_id,
        local_exchange,
        _reply_routing_header
    )
    return [
        (anti_entropy_exchange, AntiEntropyAuditRequest.routing_key, message), 
    ]

def _handle_anti_entropy_audit_reply(state, message_body):
    log = logging.getLogger("_handle_anti_entropy_audit_reply")
    message = AntiEntropyAuditReply.unmarshall(message_body)
    assert message.request_id == state["request-id"]

    if message.error:
        info = "error %s" % (message.error_message, )
        log.error(info)
        print >> sys.stderr, info
        sys.exit(-1)

    info = "audit OK!"
    log.info(info)
    print >> sys.stderr, info
    sys.exit(0)

_dispatch_table = {
    anti_entropy_audit_reply_routing_key    : _handle_anti_entropy_audit_reply,
}

if __name__ == "__main__":
    state = dict()
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_pre_loop_function
        )
    )

