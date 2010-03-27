# -*- coding: utf-8 -*-
"""
diyapi_data_reader_main.py

Receives block read requests.
Looks up pointers to data by querying the database server
Looks for files in both the hashfanout area 
Responds with content or "not available"
"""
from collections import namedtuple
import logging
import os.path
import sys

from tools import amqp_connection
from tools import message_driven_process as process
from tools import repository

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply

_log_path = u"/var/log/pandora/diyapi_data_reader_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "data_reader"
_routing_key_binding = "data_reader.*"
_reply_routing_header = "data_reader"
_key_lookup_reply_routing_key = "%s.%s" % (
    _reply_routing_header,
    DatabaseKeyLookupReply.routing_tag,
)

_retrieve_state_tuple = namedtuple("RetrieveState", [ 
    "avatar_id",
    "key",
    "reply_exchange",
    "reply_routing_header",
    "segment_size",
    "sequence",
    "file_name",
])


def _handle_retrieve_key_start(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_start")
    message = RetrieveKeyStart.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_routing_key = "".join(
        [message.reply_routing_header, ".", RetrieveKeyStartReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message.request_id in state:
        error_string = "invalid duplicate request_id in RetrieveKeyStart"
        log.error(error_string)
        reply = RetrieveKeyStartReply(
            message.request_id,
            RetrieveKeyStartReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message.request_id] = _retrieve_state_tuple(
        avatar_id = message.avatar_id,
        key = message.key,
        reply_exchange = message.reply_exchange,
        reply_routing_header = message.reply_routing_header,
        segment_size = None,
        sequence = None,
        file_name = None
    )

    # send a lookup request to the database, with the reply
    # coming back to us
    local_exchange = amqp_connection.local_exchange_name
    database_request = DatabaseKeyLookup(
        message.request_id,
        message.avatar_id,
        local_exchange,
        _reply_routing_header,
        message.key 
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_retrieve_key_next(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_next")
    message = RetrieveKeyNext.unmarshall(message_body)

    try:
        retrieve_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return

    log.info("avatar_id = %s, key = %s sequence = %s" % (
        retrieve_state.avatar_id, retrieve_state.key, message.sequence
    ))

    reply_exchange = retrieve_state.reply_exchange
    reply_routing_key = "".join(
        [retrieve_state.reply_routing_header, 
         ".", 
         RetrieveKeyNextReply.routing_tag]
    )

    if message.sequence != retrieve_state.sequence+1:
        error_string = "%s %s out of sequence %s %s" % (
            retrieve_state.avatar_id, 
            retrieve_state.key,
            message.sequence,
            retrieve_state.sequence+1
        )
        log.error(error_string)
        reply = RetrieveKeyNextReply(
            message.request_id,
            RetrieveKeyNextReply.error_out_of_sequence,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    content_path = repository.content_path(
        retrieve_state.avatar_id, 
        retrieve_state.file_name
    ) 

    offset = message.sequence * retrieve_state.segment_size

    try:
        with open(content_path, "r") as input_file:
            input_file.seek(offset)
            data_content = input_file.read(retrieve_state.segment_size)
    except Exception, instance:
        log.exception("%s %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply = RetrieveKeyNextReply(
            message.request_id,
            RetrieveKeyNextReply.error_exception,
            error_message = str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    state[message.request_id] = retrieve_state._replace(
        sequence=message.sequence
    )

    reply = RetrieveKeyNextReply(
        message.request_id,
        RetrieveKeyNextReply.successful,
        data_content = data_content
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

def _handle_retrieve_key_final(state, message_body):
    log = logging.getLogger("_handle_retrieve_key_final")
    message = RetrieveKeyNext.unmarshall(message_body)

    try:
        retrieve_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return

    log.info("avatar_id = %s, key = %s sequence = %s" % (
        retrieve_state.avatar_id, retrieve_state.key, message.sequence
    ))

    reply_exchange = retrieve_state.reply_exchange
    reply_routing_key = "".join(
        [retrieve_state.reply_routing_header, 
         ".", 
         RetrieveKeyFinalReply.routing_tag]
    )

    if message.sequence != retrieve_state.sequence+1:
        error_string = "%s %s out of sequence %s %s" % (
            retrieve_state.avatar_id, 
            retrieve_state.key,
            message.sequence,
            retrieve_state.sequence+1
        )
        log.error(error_string)
        reply = RetrieveKeyFinalReply(
            message.request_id,
            RetrieveKeyNextReply.error_out_of_sequence,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    content_path = repository.content_path(
        retrieve_state.avatar_id, 
        retrieve_state.file_name
    ) 

    offset = message.sequence * retrieve_state.segment_size

    try:
        with open(content_path, "r") as input_file:
            input_file.seek(offset)
            data_content = input_file.read(retrieve_state.segment_size)
    except Exception, instance:
        log.exception("%s %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply = RetrieveKeyFinalReply(
            message.request_id,
            RetrieveKeyNextReply.error_exception,
            error_message = str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    # we don't save the state, because we are done

    reply = RetrieveKeyFinalReply(
        message.request_id,
        RetrieveKeyFinalReply.successful,
        data_content = data_content
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

def _handle_key_lookup_reply(state, message_body):
    log = logging.getLogger("_handle_key_lookup_reply")
    message = DatabaseKeyLookupReply.unmarshall(message_body)

    try:
        retrieve_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return

    reply_exchange = retrieve_state.reply_exchange
    reply_routing_key = "".join(
        [retrieve_state.reply_routing_header, 
         ".", 
         RetrieveKeyStartReply.routing_tag]
    )

    # if we got a database error, pass it on 
    if message.error:
        log.error("%s %s database error: (%s) %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
            message.result,
            message.error_message,
        ))
        reply = RetrieveKeyStartReply(
            message.request_id,
            RetrieveKeyStartReply.error_database,
            error_message = message.error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    content_path = repository.content_path(
        retrieve_state.avatar_id, 
        message.database_content.file_name
    ) 
    segment_size = message.database_content.segment_size
    segment_count = message.database_content.segment_count

    try:
        with open(content_path, "r") as input_file:
            data_content = input_file.read(segment_size)
    except Exception, instance:
        log.exception("%s %s" % (
            retrieve_state.avatar_id,
            retrieve_state.key,
        ))
        reply = RetrieveKeyStartReply(
            message.request_id,
            RetrieveKeyStartReply.error_exception,
            error_message = str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    # if we have more than one segment, we need to save the state
    # otherwise this request is done
    if segment_count > 1:
        state[message.request_id] = retrieve_state._replace(
            sequence=0, 
            segment_size=segment_size,
            file_name=message.database_content.file_name
        )

    reply = RetrieveKeyStartReply(
        message.request_id,
        RetrieveKeyStartReply.successful,
        message.database_content.timestamp,
        message.database_content.is_tombstone,
        message.database_content.segment_number,
        message.database_content.segment_count,
        message.database_content.segment_size,
        message.database_content.total_size,
        message.database_content.adler32,
        message.database_content.md5,
        data_content = data_content
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

_dispatch_table = {
    RetrieveKeyStart.routing_key    : _handle_retrieve_key_start,
    RetrieveKeyNext.routing_key     : _handle_retrieve_key_next,
    RetrieveKeyFinal.routing_key    : _handle_retrieve_key_final,
    _key_lookup_reply_routing_key   : _handle_key_lookup_reply,
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

