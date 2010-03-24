# -*- coding: utf-8 -*-
"""
diyapi_data_reader_main.py

Receives block read requests.
Looks up pointers to data by querying the database server
Looks for files in both the hashfanout area 
Responds with content or "not available"
"""
import logging
import sys

from tools import amqp_connection
from tools import message_driven_process as process
from tools import repository

from messages.retrieve_key import RetrieveKey
from messages.retrieve_key_reply import RetrieveKeyReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply

_log_path = u"/var/log/pandora/diyapi_data_reader.log"
_queue_name = "data_reader"
_routing_key_binding = "data_reader.*"
_key_lookup_reply_routing_key = "data_reader.database_key_lookup_reply"


def _handle_retrieve_key(state, message_body):
    log = logging.getLogger("_handle_retrieve_key")
    message = RetrieveKey.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    # if we already have a state entry for this request_id, something is wrong
    if message.request_id in state:
        error_string = "invalid duplicate request_id in RetrieveKey"
        log.error(error_string)
        reply = RetrieveKeyReply(
            message.request_id,
            RetrieveKeyReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, message.reply_routing_key, reply, )] 

    # save the original message in state
    state[message.request_id] = message

    # send a lookup request to the database, with the reply
    # coming back to us
    local_exchange = amqp_connection.local_exchange_name
    database_request = DatabaseKeyLookup(
        message.request_id,
        message.avatar_id,
        local_exchange,
        _key_lookup_reply_routing_key,
        message.key 
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_key_lookup_reply(state, message_body):
    log = logging.getLogger("_handle_key_lookup_reply")
    message = DatabaseKeyLookupReply.unmarshall(message_body)

    try:
        original_message = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return

    reply_exchange = original_message.reply_exchange
    reply_routing_key = original_message.reply_routing_key

    # if we got a database error, pass it on 
    if message.error:
        log.error("%s %s database error: (%s) %s" % (
            original_message.avatar_id,
            original_message.key,
            message.result,
            message.error_message,
        ))
        reply = RetrieveKeyReply(
            message.request_id,
            RetrieveKeyReply.error_database,
            error_message = message.error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    content_path = repository.content_path(
        original_message.avatar_id, 
        message.unmarshalled_content.file_name
    ) 

    try:
        with open(content_path, "r") as input_file:
            data_content = input_file.read()
    except Exception, instance:
        log.exception("%s %s" % (
            original_message.avatar_id,
            original_message.key,
        ))
        reply = RetrieveKeyReply(
            message.request_id,
            RetrieveKeyReply.error_exception,
            error_message = str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    reply = RetrieveKeyReply(
        message.request_id,
        RetrieveKeyReply.successful,
        database_content = message.database_content,
        data_content = data_content
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

_dispatch_table = {
    RetrieveKey.routing_key         : _handle_retrieve_key,
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

