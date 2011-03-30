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
from diyapi_tools.persistent_state import load_state, save_state
from diyapi_tools import repository

from diyapi_data_writer.state_cleaner import StateCleaner
from diyapi_data_writer.heartbeater import Heartbeater

from diyapi_database_server import database_content

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_data_writer_%s.log" % (
    _local_node_name,
)
_persistent_state_file_name = "data-write-%s" % (_local_node_name, )
_database_server_address = os.environ.get(
    "DIYAPI_DATABASE_SERVER_ADDRESS",
    "tcp://127.0.0.1:8000"
)
_data_writer_address = os.environ.get(
    "DIYAPI_DATA_WRITER_ADDRESS",
    "tcp://127.0.0.1:8100"
)
_key_insert_timeout = 60.0
_key_destroy_timeout = 60.0
_key_purge_timeout = 60.0
_archive_timeout = 30 * 60.0
_heartbeat_interval = float(
    os.environ.get("DIYAPI_DATA_WRITER_HEARTBEAT", "60.0")
)

_request_state_tuple = namedtuple("RequestState", [ 
    "xrep_ident",
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

def _compute_filename(message_request_id):
    """
    compute a unique filename from message attributes
    to begin with, let's just use request_id
    """
    return message_request_id

def _handle_archive_key_entire(state, message, data):
    log = logging.getLogger("_handle_archive_key_entire")
    log.info("avatar_id = %s, key = %s" % (
        message["avatar-id"], message["key"], 
    ))

    reply = {
        "message-type"  : "archive-key-final-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

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
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"],
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
    request = {
        "message-type"      : "key-insert",
        "request-id"        : message["request-id"],
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "database-content"  : dict(database_entry._asdict().items())
    }
    state["database-client"].queue_message_for_send(request)

def _handle_archive_key_start(state, message, data):
    log = logging.getLogger("_handle_archive_key_start")
    log.info("avatar_id = %s, key = %s" % (
        message["avatar-id"], message["key"], 
    ))

    reply = {
        "message-type"  : "archive-key-start-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in ArchiveKeyEntire"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

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
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"],
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

    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply)

def _handle_archive_key_next(state, message, data):
    log = logging.getLogger("_handle_archive_key_next")

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

    reply = {
        "message-type"  : "archive-key-next-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

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
        reply["result"] = "out-of-sequence"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    try:
        with open(work_path, "a") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (request_state.avatar_id, request_state.key, ))
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = request_state._replace(
        sequence=request_state.sequence+1,
        timeout=time.time()+_archive_timeout
    )

    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply)

def _handle_archive_key_final(state, message, data):
    log = logging.getLogger("_handle_archive_key_final")

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

    reply = {
        "message-type"  : "archive-key-final-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

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
        reply["result"] = "out-of-sequence"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    try:
        with open(work_path, "a") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s %s" % (request_state.avatar_id, request_state.key, ))
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = request_state._replace(
        xrep_ident=message["xrep-ident"],
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
    request = {
        "message-type"      : "key-insert",
        "request-id"        : message["request-id"],
        "avatar-id"         : request_state.avatar_id,
        "key"               : request_state.key, 
        "database-content"  : dict(database_entry._asdict().items())
    }
    state["database-client"].queue_message_for_send(request)

def _handle_destroy_key(state, message, _data):
    log = logging.getLogger("_handle_destroy_key")
    log.info("avatar_id = %s, key = %s segment = %s" % (
        message["avatar-id"], message["key"], message["segment-number"]
    ))

    reply = {
        "message-type"  : "destroy-key-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in DestroyKey"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    file_name = _compute_filename(message["request-id"])

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"],
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
    request = {
        "message-type"      : "key-destroy",
        "request-id"        : message["request-id"],
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : message["version-number"],
        "segment-number"    : message["segment-number"],
        "timestamp"         : message["timestamp"],
    }
    state["database-client"].queue_message_for_send(request)

def _handle_purge_key(state, message, _data):
    log = logging.getLogger("_handle_purge_key")
    log.info("avatar_id = %s, key = %s segment = %s" % (
        message["avatar-id"], message["key"], message["segment-number"]
    ))

    reply = {
        "message-type"  : "purge-key-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request_id, something is wrong
    if message["request-id"] in state:
        error_string = "invalid duplicate request_id in PurgeKey"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error_message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    file_name = _compute_filename(message["request-id"])

    # save stuff we need to recall in state
    state["active-requests"][message["request-id"]] = _request_state_tuple(
        xrep_ident=message["xrep-ident"],
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
    request = {
        "message-type"      : "key-purge",
        "request-id"        : message["request-id"],
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : message["version-number"],
        "segment-number"    : message["segment-number"],
        "timestamp"         : message["timestamp"],
    }
    state["database-client"].queue_message_for_send(request)

def _handle_key_insert_reply(state, message, _data):
    log = logging.getLogger("_handle_key_insert_reply")

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply = {
        "message-type"  : "archive-key-final-reply",
        "xrep-ident"    : request_state.xrep_ident,
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "previous-size" : None
    }

    work_path = repository.content_input_path(
        request_state.avatar_id, request_state.file_name
    ) 
    content_path = repository.content_path(
        request_state.avatar_id, request_state.file_name
    ) 

    # if we got a database error, heave the data we stored
    if message["result"] != "success":
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
        reply["result"] = "database-error"
        reply["error_message"] = message["error-message"]
        state["xrep-server"].queue_message_for_send(reply)
        return

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
        reply["result"] = "exception"
        reply["error_message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return
    
    reply["result"] = "success"
    reply["previous-size"] = message["previous-size"]
    state["xrep-server"].queue_message_for_send(reply)

def _handle_key_destroy_reply(state, message, _data):
    log = logging.getLogger("_handle_key_destroy_reply")

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply = {
        "message-type"  : "destroy-key-reply",
        "xrep-ident"    : request_state.xrep_ident,
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "total-size"    : None,
    }

    # if we got a database error, DON'T heave the data we stored
    if message["result"] != "success":
        log.error("%s %s database error: (%s) %s" % (
            request_state.avatar_id,
            request_state.key,
            message["result"],
            message["error-message"],
        ))

        if message["result"] == "too-old":
            reply["result"] = "too-old"
        else:
            reply["result"] = "database-error"
        reply["error_message"] = message["error-message"]
        state["xrep-server"].queue_message_for_send(reply)
        return

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
            reply["result"] = "exception"
            reply["error_message"] = error_string
            state["xrep-server"].queue_message_for_send(reply)
            return
  
    reply["result"] = "success"
    reply["total-size"] = message["total-size"]
    state["xrep-server"].queue_message_for_send(reply)

def _handle_key_purge_reply(state, message, _data):
    log = logging.getLogger("_handle_key_purge_reply")

    try:
        request_state = state["active-requests"].pop(message["request-id"])
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (message["request-id"], ))
        return []

    reply = {
        "message-type"  : "purge-key-reply",
        "xrep-ident"    : request_state.xrep_ident,
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we got a database error, DON'T heave the data we stored
    if message["result"] != "success":
        log.error("%s %s database error: (%s) %s" % (
            request_state.avatar_id,
            request_state.key,
            message["result"],
            message["error-message"],
        ))

        if message["result"] == "no-such-key":
            reply["result"] = "no-such-key"
        else:
            reply["result"] = "database-error"
        reply["error_message"] = message["error-message"]
        state["xrep-server"].queue_message_for_send(reply)
        return

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
            reply["result"] = "exception"
            reply["error_message"] = error_string
            state["xrep-server"].queue_message_for_send(reply)
            return
  
    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply)

_dispatch_table = {
    "archive-key-entire"    : _handle_archive_key_entire,
    "archive-key-start"     : _handle_archive_key_start,
    "archive-key-next"      : _handle_archive_key_next,
    "archive-key-final"     : _handle_archive_key_final,
    "destroy-key"           : _handle_destroy_key,
    "purge-key"             : _handle_purge_key,
    "key-insert-reply"      : _handle_key_insert_reply,
    "key-destroy-reply"     : _handle_key_destroy_reply,
    "key-purge-reply"       : _handle_key_purge_reply,
}

def _create_state():
    return {
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "xrep-server"           : None,
        "database-client"       : None,
        "state-cleaner"         : None,
        "heartbeater"           : None,
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
    state["heartbeater"] = Heartbeater(state, _heartbeat_interval)

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["state-cleaner"].run, state["state-cleaner"].next_run(), ), 
        (state["heartbeater"].run, state["heartbeater"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping xrep server")
    state["xrep-server"].close()

    log.debug("stopping database client")
    state["database-client"].close()

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


