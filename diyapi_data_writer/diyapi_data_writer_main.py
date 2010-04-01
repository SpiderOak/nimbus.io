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
import time

from tools import amqp_connection
from tools.standard_logging import format_timestamp
from tools.low_traffic_thread import LowTrafficThread, low_traffic_routing_tag
from tools import message_driven_process as process
from tools import repository

from diyapi_database_server import database_content

from messages.process_status import ProcessStatus

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.destroy_key import DestroyKey
from messages.destroy_key_reply import DestroyKeyReply

from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply
from messages.database_key_destroy import DatabaseKeyDestroy
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply

_log_path = u"/var/log/pandora/diyapi_data_writer_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "data-writer-%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
_routing_header = "data_writer"
_routing_key_binding = ".".join([_routing_header, "*"])
_key_insert_reply_routing_key = ".".join([
    _routing_header,
    DatabaseKeyInsertReply.routing_tag,
])
_key_destroy_reply_routing_key = ".".join([
    _routing_header,
    DatabaseKeyDestroyReply.routing_tag,
])
_low_traffic_routing_key = ".".join([
    _routing_header, 
    low_traffic_routing_tag,
])
_key_insert_timeout = 60.0
_key_destroy_timeout = 60.0
_archive_timeout = 30 * 60.0

_archive_state_tuple = namedtuple("ArchiveState", [ 
    "timestamp",
    "timeout",
    "timeout_function",
    "avatar_id",
    "key",
    "sequence",
    "segment_number",
    "segment_size",
    "file_name",
    "reply_exchange",
    "reply_routing_header",
])

def _handle_key_insert_timeout(request_id, state):
    """called when we wait too long for a reply to a KeyInsert message"""
    log = logging.getLogger("_handle_key_insert_timeout")

    try:
        archive_state = state.pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        archive_state.avatar_id,
        archive_state.key,
        archive_state.request_id,
    ))

    # tell the caller that we're not working
    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header, 
         ".", 
         ArchiveKeyFinalReply.routing_tag]
    )
    reply = ArchiveKeyFinalReply(
        request_id,
        ArchiveKeyFinalReply.error_timeout_waiting_key_insert,
        error_message="timeout waiting for database_server"
    )
    return [(reply_exchange, reply_routing_key, reply, )] 

def _handle_key_destroy_timeout(request_id, state):
    """called when we wait too long for a reply to a KeyDestroy message"""
    log = logging.getLogger("_handle_key_destroy_timeout")

    try:
        archive_state = state.pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        archive_state.avatar_id,
        archive_state.key,
        archive_state.request_id,
    ))

    # tell the caller that we're not working
    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header, 
         ".", 
         DestroyKeyReply.routing_tag]
    )
    reply = DestroyKeyReply(
        request_id,
        DestroyKeyReply.error_timeout_waiting_key_destroy,
        error_message="timeout waiting for database_server"
    )
    return [(reply_exchange, reply_routing_key, reply, )] 

def _handle_archive_timeout(request_id, state):
    """called when we wait too long for a ArchvieKeyNext or ArchiveKeyFinal"""
    log = logging.getLogger("_handle_archive_timeout")

    try:
        archive_state = state.pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        archive_state.avatar_id,
        archive_state.key,
        archive_state.request_id,
    ))

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
        timeout=time.time()+_key_insert_timeout,
        timeout_function=_handle_key_insert_timeout,
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
        _routing_header,
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
        timeout=time.time()+_archive_timeout,
        timeout_function=_handle_archive_timeout,
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

    log.info("avatar_id = %s, key = %s sequence = %s" % (
        archive_state.avatar_id, archive_state.key, message.sequence
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
        sequence=archive_state.sequence+1,
        timeout=time.time()+_archive_timeout
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
        sequence=archive_state.sequence+1,
        timeout=time.time()+_key_insert_timeout
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
        _routing_header,
        archive_state.key, 
        database_entry
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_destroy_key(state, message_body):
    log = logging.getLogger("_handle_destroy_key")
    message = DestroyKey.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    local_exchange = amqp_connection.local_exchange_name
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DestroyKeyReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message.request_id in state:
        error_string = "invalid duplicate request_id in DestroyKey"
        log.error(error_string)
        reply = DestroyKeyReply(
            message.request_id,
            DestroyKeyReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message.request_id)

    # save stuff we need to recall in state
    state[message.request_id] = _archive_state_tuple(
        timestamp=message.timestamp,
        timeout=time.time()+_key_destroy_timeout,
        timeout_function=_handle_key_destroy_timeout,
        avatar_id=message.avatar_id,
        key=message.key,
        sequence=0,
        segment_number=0,
        segment_size=0,
        file_name=file_name,
        reply_exchange=message.reply_exchange,
        reply_routing_header=message.reply_routing_header
    )

    # send a destroy request to the database, with the reply
    # coming back to us
    database_request = DatabaseKeyDestroy(
        message.request_id,
        message.avatar_id,
        local_exchange,
        _routing_header,
        message.key, 
        message.timestamp
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

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

    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header,
         ".",
        ArchiveKeyFinalReply.routing_tag]
    )

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
        reply = ArchiveKeyFinalReply(
            message.request_id,
            ArchiveKeyFinalReply.error_database_error,
            error_message = message.error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

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
        return [(reply_exchange, reply_routing_key, reply, )]      
    
    reply = ArchiveKeyFinalReply(
        message.request_id,
        ArchiveKeyFinalReply.successful,
        message.previous_size,
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

def _handle_key_destroy_reply(state, message_body):
    log = logging.getLogger("_handle_key_destroy_reply")
    message = DatabaseKeyDestroyReply.unmarshall(message_body)

    try:
        archive_state = state.pop(message.request_id)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message.request_id, ))
        return []

    reply_exchange = archive_state.reply_exchange
    reply_routing_key = "".join(
        [archive_state.reply_routing_header,
         ".",
        DestroyKeyReply.routing_tag]
    )

    # if we got a database error, DON'T heave the data we stored
    if message.error:
        log.error("%s %s database error: (%s) %s" % (
            archive_state.avatar_id,
            archive_state.key,
            message.result,
            message.error_message,
        ))

        if message.result == DatabaseKeyDestroyReply.error_too_old:
            error_result = DestroyKeyReply.error_too_old
        else:
            error_result = DestroyKeyReply.error_database_error

        reply = DestroyKeyReply(
            message.request_id,
            error_result,
            error_message = message.error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    content_path = repository.content_path(
        archive_state.avatar_id, archive_state.file_name
    ) 

    # now heave the stored data, if it exists
    if os.path.exists(content_path):
        try:
            os.unlink(content_path)
            dirno = os.open(
                os.path.dirname(content_path), os.O_RDONLY | os.O_DIRECTORY
            )
            try:
                os.fsync(dirno)
            finally:
                os.close(dirno)    
        except Exception, instance:
            error_string = "%s %s unlinking %s %s" % (
                archive_state.avatar_id,
                archive_state.key,
                content_path,
                instance
            )
            log.exception(error_string)
            reply = DestroyKeyReply(
                message.request_id,
                DestroyKeyReply.error_exception,
                error_message = error_string
            )
            return [(reply_exchange, reply_routing_key, reply, )]      
  
    reply = DestroyKeyReply(
        message.request_id,
        DestroyKeyReply.successful,
        message.total_size
    )
    return [(reply_exchange, reply_routing_key, reply, )]      

def _handle_low_traffic(state, message_body):
    log = logging.getLogger("_handle_low_traffic")
    log.debug("ignoring low traffic message")
    return None

_dispatch_table = {
    ArchiveKeyEntire.routing_key    : _handle_archive_key_entire,
    ArchiveKeyStart.routing_key     : _handle_archive_key_start,
    ArchiveKeyNext.routing_key      : _handle_archive_key_next,
    ArchiveKeyFinal.routing_key     : _handle_archive_key_final,
    DestroyKey.routing_key          : _handle_destroy_key,
    ProcessStatus.routing_key       : _handle_process_status,
    _key_insert_reply_routing_key   : _handle_key_insert_reply,
    _key_destroy_reply_routing_key  : _handle_key_destroy_reply,
    _low_traffic_routing_key        : _handle_low_traffic,
}

def _startup(halt_event, state):
    state["low_traffic_thread"] = LowTrafficThread(
        halt_event, _routing_header
    )
    state["low_traffic_thread"].start()

    message = ProcessStatus(
        time.time(),
        amqp_connection.local_exchange_name,
        _routing_header,
        ProcessStatus.status_startup
    )

    exchange = amqp_connection.broadcast_exchange_name
    routing_key = ProcessStatus.routing_key

    return [(exchange, routing_key, message, )]

def _is_archive_state((_, value, )):
    return value.__class__.__name__ == "ArchiveState"

def _check_message_timeout(state):
    """check request_ids who are waiting for a message"""
    log = logging.getLogger("_check_message_timeout")

    state["low_traffic_thread"].reset()

    # return a (possibly empty) list of messages to send
    return_list = list()

    current_time = time.time()
    for request_id, archive_state in filter(_is_archive_state, state.items()):
        if current_time > archive_state.timeout:
            log.warn(
                "%s timed out waiting message; running timeout function" % (
                    request_id
                )
            )
            return_list.extend(
                archive_state.timeout_function(request_id, state)
            )

    return return_list

def _shutdown(state):
    state["low_traffic_thread"].join()

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
    state = {"expecting-message" : dict()}
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_startup,
            in_loop_function=_check_message_timeout,
            post_loop_function=_shutdown
        )
    )

