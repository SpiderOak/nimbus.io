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
from collections import namedtuple
import logging
import os
import sys

from tools import amqp_connection
from tools import message_driven_process as process
from tools import repository

from diyapi_database_server import database_content
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

_log_path = u"/var/log/pandora/diyapi_data_writer_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "data-writer-%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
_routing_key_binding = "data_writer.*"
_reply_routing_header = "data_writer"
_key_insert_reply_routing_key = "%s.%s" % (
    _reply_routing_header,
    DatabaseKeyInsertReply.routing_tag,
)


_archive_state_tuple = namedtuple("ArchiveState", [ 
    "timestamp",
    "avatar_id",
    "key",
    "sequence",
    "segment_number",
    "segment_size",
    "file_name",
    "reply_exchange",
    "reply_routing_header",
])

def _compute_filename(message_request_id):
    """
    compute a unique filename from message attributes
    to begin with, let's just use request_id
    """
    return message_request_id

def _handle_archive_key_entire(state, message_body):
    log = logging.getLogger("_handle_archive_key_entire")
    message = ArchiveKeyEntire.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_routing_key = "".join(
        [message.reply_routing_header, ".", ArchiveKeyFinalReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message.request_id in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply = ArchiveKeyFinalReply(
            message.request_id,
            ArchiveKeyFinalReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message.request_id)

    # store the message content in a work area
    work_path = repository.content_input_path(message.avatar_id, file_name) 
    try:
        with open(work_path, "w") as content_file:
            content_file.write(message.content)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (message.avatar_id, message.key, ))
        reply = ArchiveKeyFinalReply(
            message.request_id,
            ArchiveKeyFinalReply.error_exception,
            error_message=str(instance)
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message.request_id] = _archive_state_tuple(
        timestamp=message.timestamp,
        avatar_id=message.avatar_id,
        key=message.key,
        sequence=0,
        segment_number=message.segment_number,
        segment_size=len(message.content),
        file_name=file_name,
        reply_exchange=message.reply_exchange,
        reply_routing_header=message.reply_routing_header
    )

    # send an insert request to the database, with the reply
    # coming back to us
    database_entry = database_content.factory(
        timestamp=message.timestamp, 
        is_tombstone=False,  
        segment_number=message.segment_number,  
        segment_size=len(message.content),  
        segment_count=1,
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
        _reply_routing_header,
        message.key, 
        database_entry
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_archive_key_start(state, message_body):
    log = logging.getLogger("_handle_archive_key_start")
    message = ArchiveKeyStart.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_routing_key = "".join(
        [message.reply_routing_header, ".", ArchiveKeyStartReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message.request_id in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply = ArchiveKeyStartReply(
            message.request_id,
            ArchiveKeyStartReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message.request_id)

    # store the message content in a work area
    work_path = repository.content_input_path(message.avatar_id, file_name) 
    try:
        with open(work_path, "w") as content_file:
            content_file.write(message.data_content)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (message.avatar_id, message.key, ))
        reply = ArchiveKeyStartReply(
            message.request_id,
            ArchiveKeyStartReply.error_exception,
            error_message=str(instance)
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message.request_id] = _archive_state_tuple(
        timestamp=message.timestamp,
        avatar_id=message.avatar_id,
        key=message.key,
        sequence=message.sequence,
        segment_number=message.segment_number,
        segment_size=message.segment_size,
        file_name=file_name,
        reply_exchange=message.reply_exchange,
        reply_routing_header=message.reply_routing_header
    )

    reply = ArchiveKeyStartReply(
        message.request_id,
        ArchiveKeyStartReply.successful
    )
    return [(message.reply_exchange, reply_routing_key, reply, )] 

def _handle_archive_key_next(state, message_body):
    log = logging.getLogger("_handle_archive_key_next")
    message = ArchiveKeyNext.unmarshall(message_body)

    try:
        archive_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return []

    log.info("avatar_id = %s, key = %s" % (
        archive_state.avatar_id, archive_state.key, 
    ))

    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header, 
         ".", 
         ArchiveKeyNextReply.routing_tag]
    )
    work_path = repository.content_input_path(
        archive_state.avatar_id, archive_state.file_name
    ) 

    # is this message is out of sequence, give up on the whole thing 
    if message.sequence != archive_state.sequence+1:
        error_string = "%s %s message out of sequence %s %s" % (
            archive_state.avatar_id,
            archive_state.key,
            message.sequence,
            archive_state.sequence
        )
        log.error(error_string)
        try:
            os.unlink(work_path)
        except Exception:
            log.exception("error")
        reply = ArchiveKeyNextReply(
            message.request_id,
            ArchiveKeyNextReply.error_out_of_sequence,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    try:
        with open(work_path, "a") as content_file:
            content_file.write(message.data_content)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (archive_state.avatar_id, archive_state.key, ))
        reply = ArchiveKeyNextReply(
            message.request_id,
            ArchiveKeyNextReply.error_exception,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message.request_id] = archive_state._replace(
        sequence=archive_state.sequence+1
    )

    reply = ArchiveKeyNextReply(
        message.request_id,
        ArchiveKeyNextReply.successful
    )
    return [(reply_exchange, reply_routing_key, reply, )] 

def _handle_archive_key_final(state, message_body):
    log = logging.getLogger("_handle_archive_key_final")
    message = ArchiveKeyFinal.unmarshall(message_body)

    try:
        archive_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return []

    log.info("avatar_id = %s, key = %s" % (
        archive_state.avatar_id, archive_state.key, 
    ))

    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header, 
         ".", 
         ArchiveKeyFinalReply.routing_tag]
    )
    work_path = repository.content_input_path(
        archive_state.avatar_id, archive_state.file_name
    ) 

    # is this message is out of sequence, give up on the whole thing 
    if message.sequence != archive_state.sequence+1:
        error_string = "%s %s message out of sequence %s %s" % (
            archive_state.avatar_id,
            archive_state.key,
            message.sequence,
            archive_state.sequence
        )
        log.error(error_string)
        try:
            os.unlink(work_path)
        except Exception:
            log.exception("error")
        reply = ArchiveKeyFinalReply(
            message.request_id,
            ArchiveKeyNextReply.error_out_of_sequence,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    try:
        with open(work_path, "a") as content_file:
            content_file.write(message.data_content)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (archive_state.avatar_id, archive_state.key, ))
        reply = ArchiveKeyFinalReply(
            message.request_id,
            ArchiveKeyNextReply.error_exception,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message.request_id] = archive_state._replace(
        sequence=archive_state.sequence+1
    )

    # send an insert request to the database, with the reply
    # coming back to us
    database_entry = database_content.factory(
        timestamp=archive_state.timestamp, 
        is_tombstone=False,  
        segment_number=archive_state.segment_number,  
        segment_size=archive_state.segment_size,  
        segment_count=message.sequence+1,
        total_size=message.total_size,  
        adler32=message.adler32, 
        md5=message.md5,
        file_name=archive_state.file_name
    )
    local_exchange = amqp_connection.local_exchange_name
    database_request = DatabaseKeyInsert(
        message.request_id,
        archive_state.avatar_id,
        local_exchange,
        _reply_routing_header,
        archive_state.key, 
        database_entry
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_key_insert_reply(state, message_body):
    log = logging.getLogger("_handle_key_insert_reply")
    message = DatabaseKeyInsertReply.unmarshall(message_body)

    try:
        archive_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return []

    work_path = repository.content_input_path(
        archive_state.avatar_id, archive_state.file_name
    ) 
    content_path = repository.content_path(
        archive_state.avatar_id, archive_state.file_name
    ) 

    # if we got a database error, heave the data we stored
    if message.error:
        log.error("%s %s database error: (%s) %s removing %s" % (
            archive_state.avatar_id,
            archive_state.key,
            message.result,
            message.error_message,
            work_path
        ))
        try:
            os.unlink(work_path)
        except Exception, instance:
            log.exception("%s %s %s" % (
                archive_state.avatar_id, archive_state.key, instance
            ))    

    # move the stored message to a permanent location
    try:
        os.rename(work_path, content_path)
        dirno = os.open(
            os.path.dirname(content_path), os.O_RDONLY | os.O_DIRECTORY
        )
        try:
            os.fsync(dirno)
        finally:
            os.close(dirno)    
    except Exception, instance:
        error_string = "%s %s renaming %s to %s %s" % (
            archive_state.avatar_id,
            archive_state.key,
            work_path,
            content_path,
            instance
        )
        log.exception(error_string)
        reply = ArchiveKeyFinalReply(
            message.request_id,
            ArchiveKeyFinalReply.error_exception,
            error_message = error_string
        )
    else:
        reply = ArchiveKeyFinalReply(
            message.request_id,
            message.result,
            message.previous_size,
            message.error_message
        )

    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header,
         ".",
        ArchiveKeyFinalReply.routing_tag]
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

_dispatch_table = {
    ArchiveKeyEntire.routing_key    : _handle_archive_key_entire,
    ArchiveKeyStart.routing_key     : _handle_archive_key_start,
    ArchiveKeyNext.routing_key      : _handle_archive_key_next,
    ArchiveKeyFinal.routing_key     : _handle_archive_key_final,
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

