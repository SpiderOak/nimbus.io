# -*- coding: utf-8 -*-
"""
diyapi_handoff_server_main.py

"""
import logging
import os
import sys
import time
import uuid

from diyapi_tools import amqp_connection
from diyapi_tools import message_driven_process as process
from diyapi_tools.standard_logging import format_timestamp 

from messages.process_status import ProcessStatus

from messages.hinted_handoff import HintedHandoff
from messages.hinted_handoff_reply import HintedHandoffReply

from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.purge_key_reply import PurgeKeyReply

from diyapi_data_writer.diyapi_data_writer_main import _routing_header \
        as data_writer_routing_header

from diyapi_handoff_server.hint_repository import HintRepository
from diyapi_handoff_server.forwarder_coroutine import forwarder_coroutine

_log_path = u"/var/log/pandora/diyapi_handoff_server_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "handoff-server-%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
_routing_header = "handoff_server"
_routing_key_binding = ".".join([_routing_header, "*"])
_retrieve_key_start_reply_routing_key = ".".join([
    _routing_header, RetrieveKeyStartReply.routing_tag
])
_retrieve_key_next_reply_routing_key = ".".join([
    _routing_header, RetrieveKeyNextReply.routing_tag
])
_retrieve_key_final_reply_routing_key = ".".join([
    _routing_header, RetrieveKeyFinalReply.routing_tag
])
_archive_key_start_reply_routing_key = ".".join([
    _routing_header, ArchiveKeyStartReply.routing_tag
])
_archive_key_next_reply_routing_key = ".".join([
    _routing_header, ArchiveKeyNextReply.routing_tag
])
_archive_key_final_reply_routing_key = ".".join([
    _routing_header, ArchiveKeyFinalReply.routing_tag
])
_purge_key_reply_routing_key = ".".join([
    _routing_header, PurgeKeyReply.routing_tag
])

_active_status = [
    ProcessStatus.status_startup, ProcessStatus.status_heartbeat,
] 

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
        state["hint-repository"].store(
            message.dest_exchange,
            message.timestamp,
            message.avatar_id,
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
    
    # we're interested in startup and heartbeat messages from data_writers
    # for whom we may have handoffs
    if message.routing_header == data_writer_routing_header \
    and message.status in _active_status:
        results = _check_for_handoffs(state, message.exchange)
    else:
        results = []

    return results

def _handle_retrieve_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_start_reply")
    message = RetrieveKeyStartReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != RetrieveKeyStartReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_retrieve_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_next_reply")
    message = RetrieveKeyNextReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != RetrieveKeyNextReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_retrieve_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_final_reply")
    message = RetrieveKeyFinalReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != RetrieveKeyFinalReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_archive_key_start_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_start_reply")
    message = ArchiveKeyStartReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != ArchiveKeyStartReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_archive_key_next_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_next_reply")
    message = ArchiveKeyNextReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != ArchiveKeyNextReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_archive_key_final_reply(state, message_body):
    log = logging.getLogger("_handle_archive_key_final_reply")
    message = ArchiveKeyFinalReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != ArchiveKeyFinalReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))
    
    return state[message.request_id].send(message)

def _handle_purge_key_reply(state, message_body):
    log = logging.getLogger("_handle_purge_key_reply")
    message = PurgeKeyReply.unmarshall(message_body)

    #TODO: we need to squawk about this somehow
    if message.result != PurgeKeyReply.successful:
        log.error("%s failed (%s) %s" % (
            message.request_id, message.result, message.error_message
        ))
        if message.request_id in state:
            del state[message.request_id]            
        return []

    if message.request_id not in state:
        log.error("no state for %s" % (message.request_id, ))
        return []

    log.debug("%s result = %s" % (message.request_id, message.result, ))

    # the last thing the coroutine wants to give us is our hint
    hint = state[message.request_id].next()
    try:
        state["hint-repository"].purge_hint(hint)
    except Exception, instance:
        log.exception(instance)

    # all done
    del state[message.request_id]            
    return _check_for_handoffs(state, hint.exchange)

_dispatch_table = {
    HintedHandoff.routing_key               : _handle_hinted_handoff,
    ProcessStatus.routing_key               : _handle_process_status,
    _retrieve_key_start_reply_routing_key   : _handle_retrieve_key_start_reply,
    _retrieve_key_next_reply_routing_key    : _handle_retrieve_key_next_reply,
    _retrieve_key_final_reply_routing_key   : _handle_retrieve_key_final_reply,
    _archive_key_start_reply_routing_key    : _handle_archive_key_start_reply,
    _archive_key_next_reply_routing_key     : _handle_archive_key_next_reply,
    _archive_key_final_reply_routing_key    : _handle_archive_key_final_reply,
    _purge_key_reply_routing_key            : _handle_purge_key_reply,
}

def _check_for_handoffs(state, dest_exchange):
    """
    initiate the the process of retrieving handoffs and sending them to
    the data_writer at the destination_exchange
    """
    log = logging.getLogger("_start_returning_handoffs")
    hint = state["hint-repository"].next_hint(dest_exchange)
    if hint is None:
        return []
    request_id = uuid.uuid1().hex
    log.debug("found hint for exchange = %s assigning request_id %s" % (
        dest_exchange, request_id,  
    ))
    state[request_id] = forwarder_coroutine(request_id, hint, _routing_header) 
    return state[request_id].next()

def _startup(_halt_event, state):
    state["hint-repository"] = HintRepository()

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
    state["hint-repository"].close()
    del state["hint-repository"]

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

