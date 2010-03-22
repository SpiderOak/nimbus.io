# -*- coding: utf-8 -*-
"""
diyapi_data_writer_main.py

Stores received segments (1 for each sequence) 
in the incoming directory with a temp extension.
When final segment is received
fsyncs temp data file
renames into place,
fsyncs the directory into which the file was renamed
sends message to the database server to record key as stored.
ACK back to to requestor includes size (from the database server) 
of any previous key this key supersedes (for space accounting.)
"""
import logging
import os
import sys

from tools import amqp_connection
from tools import message_driven_process as process
from tools import repository

from diyapi_database_server import database_content
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_entire_reply import ArchiveKeyEntireReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

_log_path = u"/var/log/pandora/diyapi_data_writer.log"
_queue_name = "data_writer"
_routing_key_binding = "data_writer.*"
_key_insert_reply_routing_key = "data_writer.database_key_insert_reply"


def _compute_filename(message):
    """
    compute a unique filename from message attributes
    to begin with, let's just use request_id
    """
    return message.request_id

def _handle_archive_key_entire(state, message_body):
    log = logging.getLogger("_handle_archive_key_entire")
    message = ArchiveKeyEntire.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    # if we already have a state entry for this request_id, something is wrong
    if message.request_id in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply = ArchiveKeyEntireReply(
            message.request_id,
            ArchiveKeyEntireReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, message.reply_routing_key, reply, )] 

    file_name = _compute_filename(message)

    # store the message content in a work area
    input_path = repository.content_input_path(message.avatar_id, file_name) 
    try:
        with open(input_path, "w") as content_file:
            content_file.write(message.content)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (message.avatar_id, message.key, ))
        reply = ArchiveKeyEntireReply(
            message.request_id,
            ArchiveKeyEntireReply.error_exception,
            error_message=str(instance)
        )
        return [(message.reply_exchange, message.reply_routing_key, reply, )] 

    # save the original message and file_name in state
    state[message.request_id] = (message, file_name, )

    # send an insert request to the database, with the reply
    # coming back to us
    database_entry = database_content.factory(
        timestamp=message.timestamp, 
        is_tombstone=False,  
        segment_number=message.segment_number,  
        segment_size=len(message.content),  
        total_size=len(message.content),  
        adler32=message.adler32, 
        md5=message.md5 ,
        file_name=file_name
    )
    local_exchange = amqp_connection.local_exchange_name
    database_request = DatabaseKeyInsert(
        message.request_id,
        message.avatar_id,
        local_exchange,
        _key_insert_reply_routing_key,
        message.key, 
        database_entry
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_key_insert_reply(state, message_body):
    log = logging.getLogger("_handle_key_insert_reply")
    message = DatabaseKeyInsertReply.unmarshall(message_body)

    try:
        (original_message, file_name, ) = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return

    input_path = repository.content_input_path(
        original_message.avatar_id, file_name
    ) 
    content_path = repository.content_path(
        original_message.avatar_id, file_name
    ) 

    # if we got a database error, heave the data we stored
    if message.error:
        log.error("%s %s database error: (%s) %s removing %s" % (
            original_message.avatar_id,
            original_message.key,
            message.result,
            message.error_message,
            input_path
        ))
        try:
            os.unlink(input_path)
        except Exception, instance:
            log.exception("%s %s %s" % (
                original_message.avatar_id, original_message.key, instance
            ))    

    # move the stored message to a permanent location
    try:
        os.rename(input_path, content_path)
    except Exception, instance:
        error_string = "%s %s renaming %s to %s %s" % (
            original_message.avatar_id,
            original_message.key,
            input_path,
            content_path,
            instance
        )
        log.exception(error_string)
        reply = ArchiveKeyEntireReply(
            message.request_id,
            ArchiveKeyEntireReply.error_exception,
            error_message = error_string
        )
    else:
        reply = ArchiveKeyEntireReply(
            message.request_id,
            message.result,
            message.previous_size,
            message.error_message
        )

    reply_exchange = original_message.reply_exchange
    reply_routing_key = original_message.reply_routing_key
    return [(reply_exchange, reply_routing_key, reply, )]      

_dispatch_table = {
    ArchiveKeyEntire.routing_key    : _handle_archive_key_entire,
    _key_insert_reply_routing_key   : _handle_key_insert_reply,
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

