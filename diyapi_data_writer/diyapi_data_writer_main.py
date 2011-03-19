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
from collections import deque, namedtuple
import logging
import os
import sys
import time

import zmq

import Statgrabber

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.xrep_server import XREPServer
from diyapi_tools.xreq_client import XREQClient
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.standard_logging import format_timestamp
from diyapi_tools.persistent_state import load_state, save_state
from diyapi_tools import repository

from diyapi_data_writer.state_cleaner import StateCleaner

from diyapi_database_server import database_content

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_data_writer_%s.log" % (
    _local_node_name,
)
_persistent_state_file_name = "data-write-%s" % (local_node_name, )
_database_server_address = os.environ.get(
    "DIYAPI_DATABASE_SERVER_ADDRESS",
    "ipc:///tmp/diyapi-database-server-%s/socket" % (_local_node_name, )
)
_data_writer_address = os.environ.get(
    "DIYAPI_DATA_WRITER_ADDRESS",
    "ipc:///tmp/diyapi-data-writer-%s/socket" % (_local_node_name, )
)
_key_insert_timeout = 60.0
_key_destroy_timeout = 60.0
_key_purge_timeout = 60.0
_archive_timeout = 30 * 60.0
_heartbeat_interval = float(
    os.environ.get("DIYAPI_DATA_WRITER_HEARTBEAT", "60.0")
)

_request_state_tuple = namedtuple("RequestState", [ 
    "xrep_ident"
    "timestamp",
    "timeout",
    "timeout_message",
    "avatar_id",
    "key",
    "sequence",
    "version_number",
    "segment_number",
    "segment_size",
    "file_name",
])

def _next_heartbeat_time():
    return time.time() + _heartbeat_interval

def _handle_key_insert_timeout(request_id, state):
    """called when we wait too long for a reply to a KeyInsert message"""
    log = logging.getLogger("_handle_key_insert_timeout")

    try:
        request_state = state["active-requests"].pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        request_state.avatar_id,
        request_state.key,
        request_id,
    ))

    # tell the caller that we're not working
    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header, 
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
        request_state = state["active-requests"].pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        request_state.avatar_id,
        request_state.key,
        request_id,
    ))

    # tell the caller that we're not working
    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header, 
         ".", 
         DestroyKeyReply.routing_tag]
    )
    reply = DestroyKeyReply(
        request_id,
        DestroyKeyReply.error_timeout_waiting_key_destroy,
        error_message="timeout waiting for database_server"
    )
    return [(reply_exchange, reply_routing_key, reply, )] 

def _handle_key_purge_timeout(request_id, state):
    """called when we wait too long for a reply to a KeyPurge message"""
    log = logging.getLogger("_handle_key_purge_timeout")

    try:
        request_state = state["active-requests"].pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        request_state.avatar_id,
        request_state.key,
        request_id,
    ))

    # tell the caller that we're not working
    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header, 
         ".", 
         PurgeKeyReply.routing_tag]
    )
    reply = PurgeKeyReply(
        request_id,
        PurgeKeyReply.error_timeout_waiting_key_purge,
        error_message="timeout waiting for database_server"
    )
    return [(reply_exchange, reply_routing_key, reply, )] 

def _handle_archive_timeout(request_id, state):
    """called when we wait too long for a ArchvieKeyNext or ArchiveKeyFinal"""
    log = logging.getLogger("_handle_archive_timeout")

    try:
        request_state = state["active-requests"].pop(request_id)
    except KeyError:
        log.error("can't find %s in state" % (request_id, ))
        return []

    log.error("timeout: %s %s %s" % (
        request_state.avatar_id,
        request_state.key,
        request_id,
    ))

    # we can't send a reply to the caller here because they're the ones who
    # timed out
    return []

def _compute_filename(message_request_id):
    """
    compute a unique filename from message attributes
    to begin with, let's just use request_id
    """
    return message_request_id

def _handle_archive_key_entire(state, message, data):
    log = logging.getLogger("_handle_archive_key_entire")
    message = ArchiveKeyEntire.unmarshall(message, _data)
    log.info("avatar_id = %s, key = %s" % (message["avatar-id"], message["key"], ))

    reply_routing_key = "".join(
        [message.reply_routing_header, ".", ArchiveKeyFinalReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply = ArchiveKeyFinalReply(
            message["request-id"],
            ArchiveKeyFinalReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message["request-id"])

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    # store the message content in a work area
    work_path = repository.content_input_path(message["avatar-id"], file_name) 
    try:
        with open(work_path, "w") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (message["avatar-id"], message["key"], ))
        reply = ArchiveKeyFinalReply(
            message["request-id"],
            ArchiveKeyFinalReply.error_exception,
            error_message=str(instance)
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"]
        timestamp=message["timestamp"],
        timeout=time.time()+_key_insert_timeout,
        timeout_message="archive-key-final-reply",
        avatar_id=message["avatar-id"],
        key=message["key"],
        sequence=0,
        version_number=message["version-number"],
        segment_number=message["segment-number"],
        segment_size=len(data),
        file_name=file_name,
    )

    # send an insert request to the database, with the reply
    # coming back to us
    database_entry = database_content.factory(
        timestamp=message["timestamp"], 
        is_tombstone=False,  
        segment_number=message["segment-number"],  
        segment_size=len(data),  
        version_number=message["version-number"],
        segment_count=1,
        total_size=message["total-size"],  
        file_adler32=message["file-adler32"], 
        file_md5=message["file-md5"] ,
        segment_adler32=message["segment-adler32"], 
        segment_md5=message["segment-md5"],
        file_name=file_name
    )
    local_exchange = amqp_connection.local_exchange_name
    database_request = DatabaseKeyInsert(
        message["request-id"],
        message["avatar-id"],
        local_exchange,
        _routing_header,
        message["key"], 
        database_entry
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_archive_key_start(state, message, _data):
    log = logging.getLogger("_handle_archive_key_start")
    message = ArchiveKeyStart.unmarshall(message, _data)
    log.info("avatar_id = %s, key = %s" % (message["avatar-id"], message["key"], ))

    reply_routing_key = "".join(
        [message.reply_routing_header, ".", ArchiveKeyStartReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply = ArchiveKeyStartReply(
            message["request-id"],
            ArchiveKeyStartReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message["request-id"])

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    # store the message content in a work area
    work_path = repository.content_input_path(message["avatar-id"], file_name) 
    try:
        with open(work_path, "w") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (message["avatar-id"], message["key"], ))
        reply = ArchiveKeyStartReply(
            message["request-id"],
            ArchiveKeyStartReply.error_exception,
            error_message=str(instance)
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"]
        timestamp=message["timestamp"],
        timeout=time.time()+_archive_timeout,
        timeout_message=None,
        avatar_id=message["avatar-id"],
        key=message["key"],
        sequence=message["sequence"],
        version_number=message["version-number"],
        segment_number=message["segment-number"],
        segment_size=message["segment-size"],
        file_name=file_name,
    )

    reply = ArchiveKeyStartReply(
        message["request-id"],
        ArchiveKeyStartReply.successful
    )
    return [(message.reply_exchange, reply_routing_key, reply, )] 

def _handle_archive_key_next(state, message, _data):
    log = logging.getLogger("_handle_archive_key_next")
    message = ArchiveKeyNext.unmarshall(message, _data)

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    log.info("avatar_id = %s, key = %s sequence = %s" % (
        request_state.avatar_id, request_state.key, message["sequence"]
    ))

    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header, 
         ".", 
         ArchiveKeyNextReply.routing_tag]
    )
    work_path = repository.content_input_path(
        request_state.avatar_id, request_state.file_name
    ) 

    # is this message is out of sequence, give up on the whole thing 
    if message["sequence"] != request_state.sequence+1:
        error_string = "%s %s message out of sequence %s %s" % (
            request_state.avatar_id,
            request_state.key,
            message["sequence"],
            request_state.sequence
        )
        log.error(error_string)
        try:
            os.unlink(work_path)
        except Exception:
            log.exception("error")
        reply = ArchiveKeyNextReply(
            message["request-id"],
            ArchiveKeyNextReply.error_out_of_sequence,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    try:
        with open(work_path, "a") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (request_state.avatar_id, request_state.key, ))
        reply = ArchiveKeyNextReply(
            message["request-id"],
            ArchiveKeyNextReply.error_exception,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message["request-id"]] = request_state._replace(
        sequence=request_state.sequence+1,
        timeout=time.time()+_archive_timeout
    )

    reply = ArchiveKeyNextReply(
        message["request-id"],
        ArchiveKeyNextReply.successful
    )
    return [(reply_exchange, reply_routing_key, reply, )] 

def _handle_archive_key_final(state, message, data):
    log = logging.getLogger("_handle_archive_key_final")
    message = ArchiveKeyFinal.unmarshall(message, _data)

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    log.info("avatar_id = %s, key = %s" % (
        request_state.avatar_id, request_state.key, 
    ))

    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header, 
         ".", 
         ArchiveKeyFinalReply.routing_tag]
    )
    work_path = repository.content_input_path(
        request_state.avatar_id, request_state.file_name
    ) 

    # is this message is out of sequence, give up on the whole thing 
    if message["sequence"] != request_state.sequence+1:
        error_string = "%s %s message out of sequence %s %s" % (
            request_state.avatar_id,
            request_state.key,
            message["sequence"],
            request_state.sequence
        )
        log.error(error_string)
        try:
            os.unlink(work_path)
        except Exception:
            log.exception("error")
        reply = ArchiveKeyFinalReply(
            message["request-id"],
            ArchiveKeyNextReply.error_out_of_sequence,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    try:
        with open(work_path, "a") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (request_state.avatar_id, request_state.key, ))
        reply = ArchiveKeyFinalReply(
            message["request-id"],
            ArchiveKeyNextReply.error_exception,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )] 

    # save stuff we need to recall in state
    state[message["request-id"]] = request_state._replace(
        sequence=request_state.sequence+1,
        timeout=time.time()+_key_insert_timeout
    )

    # send an insert request to the database, with the reply
    # coming back to us
    database_entry = database_content.factory(
        is_tombstone=False,  
        timestamp=request_state.timestamp, 
        version_number=request_state.version_number,
        segment_number=request_state.segment_number,  
        segment_size=request_state.segment_size,  
        segment_count=message["sequence"]+1,
        total_size=message["total-size"],  
        file_adler32=message["file-adler32"], 
        file_md5=message["file-md5"],
        segment_adler32=message["segment-adler32"], 
        segment_md5=message["segment-md5"],
        file_name=request_state.file_name
    )
    local_exchange = amqp_connection.local_exchange_name
    database_request = DatabaseKeyInsert(
        message["request-id"],
        request_state.avatar_id,
        local_exchange,
        _routing_header,
        request_state.key, 
        database_entry
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_destroy_key(state, message, _data):
    log = logging.getLogger("_handle_destroy_key")
    message = DestroyKey.unmarshall(message, _data)
    log.info("avatar_id = %s, key = %s segment = %s" % (
        message["avatar-id"], message["key"], message["segment-number"]
    ))

    local_exchange = amqp_connection.local_exchange_name
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DestroyKeyReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in DestroyKey"
        log.error(error_string)
        reply = DestroyKeyReply(
            message["request-id"],
            DestroyKeyReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message["request-id"])

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"]
        timestamp=message["timestamp"],
        timeout=time.time()+_key_destroy_timeout,
        timeout_message="destroy-key-reply",
        avatar_id=message["avatar-id"],
        key=message["key"],
        sequence=0,
        version_number=0,
        segment_number=0,
        segment_size=0,
        file_name=file_name,
    )

    # send a destroy request to the database, with the reply
    # coming back to us
    database_request = DatabaseKeyDestroy(
        message["request-id"],
        message["avatar-id"],
        local_exchange,
        _routing_header,
        message["timestamp"],
        message["key"], 
        message["version-number"],
        message["segment-number"],
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_purge_key(state, message, _data):
    log = logging.getLogger("_handle_purge_key")
    message = PurgeKey.unmarshall(message, _data)
    log.info("avatar_id = %s, key = %s segment = %s" % (
        message["avatar-id"], message["key"], message["segment-number"]
    ))

    local_exchange = amqp_connection.local_exchange_name
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", PurgeKeyReply.routing_tag]
    )

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in PurgeKey"
        log.error(error_string)
        reply = PurgeKeyReply(
            message["request-id"],
            PurgeKeyReply.error_invalid_duplicate,
            error_message=error_string
        )
        return [(message.reply_exchange, reply_routing_key, reply, )] 

    file_name = _compute_filename(message["request-id"])

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"]
        timestamp=message["timestamp"],
        timeout=time.time()+_key_purge_timeout,
        timeout_message="purge-key-reply",
        avatar_id=message["avatar-id"],
        key=message["key"],
        sequence=0,
        version_number=0,
        segment_number=0,
        segment_size=0,
        file_name=file_name,
    )

    # send a purge request to the database, with the reply
    # coming back to us
    database_request = DatabaseKeyPurge(
        message["request-id"],
        message["avatar-id"],
        local_exchange,
        _routing_header,
        message["timestamp"],
        message["key"], 
        message["version-number"],
        message["segment-number"],
    )
    return [(local_exchange, database_request.routing_key, database_request, )]

def _handle_key_insert_reply(state, message, _data):
    log = logging.getLogger("_handle_key_insert_reply")
    message = DatabaseKeyInsertReply.unmarshall(message, _data)

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header,
         ".",
        ArchiveKeyFinalReply.routing_tag]
    )

    work_path = repository.content_input_path(
        request_state.avatar_id, request_state.file_name
    ) 
    content_path = repository.content_path(
        request_state.avatar_id, request_state.file_name
    ) 

    # if we got a database error, heave the data we stored
    if message["result"] != "success:
        log.error("%s %s database error: (%s) %s removing %s" % (
            request_state.avatar_id,
            request_state.key,
            message["result"],
            message["error-message"],
            work_path
        ))
        try:
            os.unlink(work_path)
        except Exception, instance:
            log.exception("%s %s %s" % (
                request_state.avatar_id, request_state.key, instance
            ))    
        reply = ArchiveKeyFinalReply(
            message["request-id"],
            ArchiveKeyFinalReply.error_database_error,
            error_message = message["error-message"]
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
            request_state.avatar_id,
            request_state.key,
            work_path,
            content_path,
            instance
        )
        log.exception(error_string)
        reply = ArchiveKeyFinalReply(
            message["request-id"],
            ArchiveKeyFinalReply.error_exception,
            error_message = error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )]      
    
    reply = ArchiveKeyFinalReply(
        message["request-id"],
        ArchiveKeyFinalReply.successful,
        message["previous-size"],
    )

    return [(reply_exchange, reply_routing_key, reply, )]      

def _handle_key_destroy_reply(state, message, _data):
    log = logging.getLogger("_handle_key_destroy_reply")
    message = DatabaseKeyDestroyReply.unmarshall(message, _data)

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header,
         ".",
        DestroyKeyReply.routing_tag]
    )

    # if we got a database error, DON'T heave the data we stored
    if message["result"] != "success":
        log.error("%s %s database error: (%s) %s" % (
            request_state.avatar_id,
            request_state.key,
            message["result"],
            message["error-message"],
        ))

        if message["result"] == DatabaseKeyDestroyReply.error_too_old:
            error_result = DestroyKeyReply.error_too_old
        else:
            error_result = DestroyKeyReply.error_database_error

        reply = DestroyKeyReply(
            message["request-id"],
            error_result,
            error_message = message["error-message"]
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    content_path = repository.content_path(
        request_state.avatar_id, request_state.file_name
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
                request_state.avatar_id,
                request_state.key,
                content_path,
                instance
            )
            log.exception(error_string)
            reply = DestroyKeyReply(
                message["request-id"],
                DestroyKeyReply.error_exception,
                error_message = error_string
            )
            return [(reply_exchange, reply_routing_key, reply, )]      
  
    reply = DestroyKeyReply(
        message["request-id"],
        DestroyKeyReply.successful,
        message["total-size"]
    )
    return [(reply_exchange, reply_routing_key, reply, )]      

def _handle_key_purge_reply(state, message, _data):
    log = logging.getLogger("_handle_key_purge_reply")
    message = DatabaseKeyPurgeReply.unmarshall(message, _data)

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply_exchange = request_state.reply_exchange
    reply_routing_key = "".join(
        [request_state.reply_routing_header,
         ".",
        PurgeKeyReply.routing_tag]
    )

    # if we got a database error, DON'T heave the data we stored
    if message["result"] != "success":
        log.error("%s %s database error: (%s) %s" % (
            request_state.avatar_id,
            request_state.key,
            message["result"],
            message["error-message"],
        ))

        if message["result"] == DatabaseKeyPurgeReply.error_key_not_found:
            error_result = PurgeKeyReply.error_key_not_found
        else:
            error_result = PurgeKeyReply.error_database_error

        reply = PurgeKeyReply(
            message["request-id"],
            error_result,
            error_message = message["error-message"]
        )
        return [(reply_exchange, reply_routing_key, reply, )]      

    content_path = repository.content_path(
        request_state.avatar_id, request_state.file_name
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
                request_state.avatar_id,
                request_state.key,
                content_path,
                instance
            )
            log.exception(error_string)
            reply = PurgeKeyReply(
                message["request-id"],
                PurgeKeyReply.error_exception,
                error_message = error_string
            )
            return [(reply_exchange, reply_routing_key, reply, )]      
  
    reply = PurgeKeyReply(message["request-id"], PurgeKeyReply.successful)
    return [(reply_exchange, reply_routing_key, reply, )]      

_dispatch_table = {
    ArchiveKeyEntire.routing_key    : _handle_archive_key_entire,
    ArchiveKeyStart.routing_key     : _handle_archive_key_start,
    ArchiveKeyNext.routing_key      : _handle_archive_key_next,
    ArchiveKeyFinal.routing_key     : _handle_archive_key_final,
    DestroyKey.routing_key          : _handle_destroy_key,
    PurgeKey.routing_key            : _handle_purge_key,
    ProcessStatus.routing_key       : _handle_process_status,
    _key_insert_reply_routing_key   : _handle_key_insert_reply,
    _key_destroy_reply_routing_key  : _handle_key_destroy_reply,
}

def _is_request_state((_, value, )):
    return value.__class__.__name__ == "ArchiveState"

def _check_time(state):
    """check request_ids who are waiting for a message"""
    log = logging.getLogger("_check_time")
    current_time = time.time()

    for request_id, request_state in filter(_is_request_state, state.items()):
        if current_time > request_state.timeout:
            log.warn(
                "%s timed out waiting message; running timeout function" % (
                    request_id
                )
            )
            return_list.extend(
                request_state.timeout_function(request_id, state)
            )

    if current_time > state["next-heartbeat-time"]:
        log.debug("sending heartbeat. next heartbeat %s seconds" % (
            _heartbeat_interval,
        ))
        message = ProcessStatus(
            time.time(),
            amqp_connection.local_exchange_name,
            _routing_header,
            ProcessStatus.status_heartbeat
        )

        exchange = amqp_connection.broadcast_exchange_name
        routing_key = ProcessStatus.routing_key

        return_list.append(
            (exchange, routing_key, message, )
        )

        state["next-heartbeat-time"] = _next_heartbeat_time()

    return return_list

if __name__ == "__main__":
    state = dict()
    pickleable_state = load_state(_queue_name)
    if pickleable_state is not None:
        for key, value in pickleable_state:
            state[key] = _request_state_tuple(**value)

    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_startup,
            in_loop_function=_check_time,
            post_loop_function=_shutdown
        )
    )
    
def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "xrep-server"           : None,
        "database-client"       : None,
        "state-cleaner"         : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
        "active-requests"       : dict(),
    }

def _setup(_halt_event, state):
    log = logging.getLogger("_setup")

    pickleable_state = load_state(_persistent_state_file_name)
    if pickleable_state is not None:
        for key, value in pickleable_state:
            state["active-requests"][key] = _request_state_tuple(**value)

    log.info("binding xrep-server to %s" % (_data_writer_address, ))
    state["xrep-server"] = XREPServer(
        state["zmq-context"],
        _data_writer_address,
        state["receive-queue"]
    )
    state["xrep-server"].register(state["pollster"])

    state["database-client"] = XREQClient(
        state["zmq-context"],
        _database_server_address,
        state["receive-queue"]
    )
    state["database-client"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["state-cleaner"] = StateCleaner(state)

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["state-cleaner"].run, state["state-cleaner"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping xrep server")
    state["xrep-server"].close()

    state["zmq-context"].term()

    log.info("saving state")
    pickleable_state = dict()
    for request_id, request_state in state["active-requests"].items():
        pickleable_state[request_id] = request_state._asdict()

    save_state(pickleable_state, _persistent_state_file_name)
    log.debug("teardown complete")

if __name__ == "__main__":
    state = _create_state()
    sys.exit(
        time_queue_driven_process.main(
            _log_path,
            state,
            pre_loop_actions=[_setup, ],
            post_loop_actions=[_tear_down, ]
        )
    )


