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
from base64 import b64decode
from collections import deque, namedtuple
from hashlib import md5
import logging
import os
import sys
import time

import zmq

import Statgrabber

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.resilient_server import ResilientServer
from diyapi_tools.resilient_client import ResilientClient
from diyapi_tools.pull_server import PULLServer
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
_client_tag = "data-writer-%s" % (_local_node_name, )
_database_server_local_address = os.environ.get(
    "DIYAPI_DATABASE_SERVER_LOCAL_ADDRESS",
    "ipc:///tmp/spideroak-diyapi-database-server-%s/socket" % (
        _local_node_name,
    )
)
_data_writer_address = os.environ.get(
    "DIYAPI_DATA_WRITER_ADDRESS",
    "tcp://127.0.0.1:8100"
)
_data_writer_pipeline_address = os.environ.get(
    "DIYAPI_DATA_WRITER_PIPELINE_ADDRESS",
    "ipc:///tmp/spideroak-diyapi-data-writer-pipeline-%s/socket" % (
        _local_node_name,
    )
)
_data_writer_pub_address = os.environ.get(
    "DIYAPI_DATA_WRITER_PUB_ADDRESS",
    "tcp://127.0.0.1:8102"
)
_key_insert_timeout = 60.0
_key_destroy_timeout = 60.0
_key_purge_timeout = 60.0
_archive_timeout = 30 * 60.0
_heartbeat_interval = float(
    os.environ.get("DIYAPI_DATA_WRITER_HEARTBEAT", "60.0")
)

_request_state_tuple = namedtuple("RequestState", [ 
    "client_tag",
    "message_id",
    "timestamp",
    "timeout",
    "timeout_message",
    "sequence",
    "version_number",
    "segment_number",
    "segment_size",
    "file_name",
])

def _compute_filename(message):
    """
    compute a unique filename from message attributes
    to begin with, let's just use a hash of avatar-id and key
    """
    hasher = md5()
    hasher.update(str(message["avatar-id"]))
    hasher.update(message["key"])
    return hasher.hexdigest()

def _compute_state_key(message):
    """
    compute a key to the state for this message
    """
    return (message["avatar-id"], message["key"], message["segment-number"], )

def _handle_archive_key_entire(state, message, data):
    log = logging.getLogger("_handle_archive_key_entire")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    reply = {
        "message-type"  : "archive-key-final-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request
    # assume we are doing some form of restart
    if state_key in state["active-requests"]:
        log.warn("duplicate state_key (%s) assuming restart" % (state_key, ))
        del state["active-requests"][state_key]

    file_name = _compute_filename(message)

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
        log.exception("%s" % (state_key, ))
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][state_key] = _request_state_tuple(
        client_tag=message["client-tag"],
        message_id=message["message-id"],
        timestamp=message["timestamp"],
        timeout=time.time()+_key_insert_timeout,
        timeout_message="archive-key-final-reply",
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
        file_md5=b64decode(message["file-md5"]),
        segment_adler32=message["segment-adler32"], 
        segment_md5=b64decode(message["segment-md5"]),
        file_name=file_name
    )
    request = {
        "message-type"      : "key-insert",
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : message["version-number"],
        "segment-number"    : message["segment-number"],
    }
    state["database-client"].queue_message_for_send(
        request, data=database_content.marshall(database_entry)
    )

def _handle_archive_key_start(state, message, data):
    log = logging.getLogger("_handle_archive_key_start")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    reply = {
        "message-type"  : "archive-key-start-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request
    # assume we are doing some form of restart
    if state_key in state["active-requests"]:
        log.warn("duplicate state_key (%s) assuming restart" % (state_key, ))
        del state["active-requests"][state_key]

    file_name = _compute_filename(message)

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
        log.exception("%s" % (state_key, ))
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][state_key] = _request_state_tuple(
        client_tag=message["client-tag"],
        message_id=message["message-id"],
        timestamp=message["timestamp"],
        timeout=time.time()+_archive_timeout,
        timeout_message=None,
        sequence=message["sequence"],
        version_number=message["version-number"],
        segment_number=message["segment-number"],
        segment_size=message["segment-size"],
        file_name=file_name,
    )

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_next(state, message, data):
    log = logging.getLogger("_handle_archive_key_next")
    state_key = _compute_state_key(message)
    log.info("%s sequence %s" % (state_key, message["sequence"], ))

    try:
        request_state = state["active-requests"].pop(state_key)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (state_key, ))
        return []

    reply = {
        "message-type"  : "archive-key-next-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    work_path = repository.content_input_path(
        message["avatar-id"], request_state.file_name
    ) 

    # is this message is out of sequence, give up on the whole thing 
    if message["sequence"] != request_state.sequence+1:
        error_string = "%s %s message out of sequence %s %s" % (
            message["avatar-id"],
            message["key"],
            message["sequence"],
            request_state.sequence
        )
        log.error(error_string)
        try:
            os.unlink(work_path)
        except Exception:
            log.exception("error")
        reply["result"] = "out-of-sequence"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    try:
        with open(work_path, "a") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s" % (state_key, ))
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][state_key] = request_state._replace(
        sequence=request_state.sequence+1,
        timeout=time.time()+_archive_timeout
    )

    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

def _handle_archive_key_final(state, message, data):
    log = logging.getLogger("_handle_archive_key_final")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    try:
        request_state = state["active-requests"].pop(state_key)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (state_key, ))
        return []

    reply = {
        "message-type"  : "archive-key-final-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    work_path = repository.content_input_path(
        message["avatar-id"], request_state.file_name
    ) 

    # is this message is out of sequence, give up on the whole thing 
    if message["sequence"] != request_state.sequence+1:
        error_string = "%s message out of sequence %s %s" % (
            state_key,
            message["sequence"],
            request_state.sequence
        )
        log.error(error_string)
        try:
            os.unlink(work_path)
        except Exception:
            log.exception("error")
        reply["result"] = "out-of-sequence"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    Statgrabber.accumulate('diy_write_requests', 1)
    Statgrabber.accumulate('diy_write_bytes', len(data))

    try:
        with open(work_path, "a") as content_file:
            content_file.write(data)
            content_file.flush()
            os.fsync(content_file.fileno())
    except Exception, instance:
        log.exception("%s" % (state_key, ))
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return

    # save stuff we need to recall in state
    state["active-requests"][state_key] = request_state._replace(
        message_id=message["message-id"],
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
        file_md5=b64decode(message["file-md5"]),
        segment_adler32=message["segment-adler32"], 
        segment_md5=b64decode(message["segment-md5"]),
        file_name=request_state.file_name
    )
    request = {
        "message-type"      : "key-insert",
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : request_state.version_number,
        "segment-number"    : request_state.segment_number,
    }
    state["database-client"].queue_message_for_send(
        request, data=database_content.marshall(database_entry)
    )

def _handle_destroy_key(state, message, _data):
    log = logging.getLogger("_handle_destroy_key")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    reply = {
        "message-type"  : "destroy-key-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request, something is wrong
    if state_key in state["active-requests"]:
        error_string = "invalid duplicate in destroy-key"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    file_name = _compute_filename(message)

    # save stuff we need to recall in state
    state["active-requests"][state_key] = _request_state_tuple(
        client_tag=message["client-tag"],
        message_id=message["message-id"],
        timestamp=message["timestamp"],
        timeout=time.time()+_key_destroy_timeout,
        timeout_message="destroy-key-reply",
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
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : message["version-number"],
        "segment-number"    : message["segment-number"],
        "timestamp"         : message["timestamp"],
    }
    state["database-client"].queue_message_for_send(request)

def _handle_purge_key(state, message, _data):
    log = logging.getLogger("_handle_purge_key")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    reply = {
        "message-type"  : "purge-key-reply",
        "client-tag"    : message["client-tag"],
        "message-id"    : message["message-id"],
        "result"        : None,
        "error-message" : None,
    }

    # if we already have a state entry for this request, something is wrong
    if state_key in state["active-requests"]:
        error_string = "invalid duplicate request in purge-key"
        log.error(error_string)
        reply["result"] = "invalid-duplicate"
        reply["error-message"] = error_string
        state["resilient-server"].send_reply(reply)
        return

    file_name = _compute_filename(message)

    # save stuff we need to recall in state
    state["active-requests"][state_key] = _request_state_tuple(
        client_tag=message["client-tag"],
        message_id=message["message-id"],
        timestamp=message["timestamp"],
        timeout=time.time()+_key_purge_timeout,
        timeout_message="purge-key-reply",
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
        "avatar-id"         : message["avatar-id"],
        "key"               : message["key"], 
        "version-number"    : message["version-number"],
        "segment-number"    : message["segment-number"],
        "timestamp"         : message["timestamp"],
    }
    state["database-client"].queue_message_for_send(request)

def _handle_key_insert_reply(state, message, _data):
    log = logging.getLogger("_handle_key_insert_reply")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    try:
        request_state = state["active-requests"].pop(state_key)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (state_key, ))
        return []

    reply = {
        "message-type"  : "archive-key-final-reply",
        "client-tag"    : request_state.client_tag,
        "message-id"    : request_state.message_id,
        "result"        : None,
        "error-message" : None,
        "previous-size" : None
    }

    work_path = repository.content_input_path(
        message["avatar-id"], request_state.file_name
    ) 
    content_path = repository.content_path(
        message["avatar-id"], request_state.file_name
    ) 

    # if we got a database error, heave the data we stored
    if message["result"] != "success":
        log.error("%s database error: (%s) %s removing %s" % (
            state_key,
            message["result"],
            message["error-message"],
            work_path
        ))
        try:
            os.unlink(work_path)
        except Exception, instance:
            log.exception("%s %s" % (state_key, instance))    

        reply["result"] = "database-error"
        reply["error-message"] = message["error-message"]
        state["resilient-server"].send_reply(reply)
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
        error_string = "%s renaming %s to %s %s" % (
            state_key,
            work_path,
            content_path,
            instance
        )
        log.exception(error_string)
        reply["result"] = "exception"
        reply["error-message"] = str(instance)
        state["resilient-server"].send_reply(reply)
        return
    
    reply["result"] = "success"
    reply["previous-size"] = message["previous-size"]
    state["resilient-server"].send_reply(reply)

def _handle_key_destroy_reply(state, message, _data):
    log = logging.getLogger("_handle_key_destroy_reply")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    try:
        request_state = state["active-requests"].pop(state_key)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (state_key, ))
        return []

    reply = {
        "message-type"  : "destroy-key-reply",
        "client-tag"    : request_state.client_tag,
        "message-id"    : request_state.message_id,
        "result"        : None,
        "error-message" : None,
        "total-size"    : None,
    }

    # if we got a database error, DON'T heave the data we stored
    if message["result"] != "success":
        log.error("%s database error: (%s) %s" % (
            state_key,
            message["result"],
            message["error-message"],
        ))

        if message["result"] == "too-old":
            reply["result"] = "too-old"
        else:
            reply["result"] = "database-error"
        reply["error-message"] = message["error-message"]
        state["resilient-server"].send_reply(reply)
        return

    content_path = repository.content_path(
        message["avatar-id"], request_state.file_name
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
            error_string = "%s unlinking %s %s" % (
                state_key,
                content_path,
                instance
            )
            log.exception(error_string)
            reply["result"] = "exception"
            reply["error-message"] = error_string
            state["resilient-server"].send_reply(reply)
            return
  
    reply["result"] = "success"
    reply["total-size"] = message["total-size"]
    state["resilient-server"].send_reply(reply)

def _handle_key_purge_reply(state, message, _data):
    log = logging.getLogger("_handle_key_purge_reply")
    state_key = _compute_state_key(message)
    log.info("%s" % (state_key, ))

    try:
        request_state = state["active-requests"].pop(state_key)
    except KeyError:
        # if we don't have any state for this message body, there's nobody we 
        # can complain too
        log.error("No state for %r" % (state_key, ))
        return []

    reply = {
        "message-type"  : "purge-key-reply",
        "client-tag"    : request_state.client_tag,
        "message-id"    : request_state.message_id,
        "result"        : None,
        "error-message" : None,
    }

    # if we got a database error, DON'T heave the data we stored
    if message["result"] != "success":
        log.error("%s database error: (%s) %s" % (
            state_key,
            message["result"],
            message["error-message"],
        ))

        if message["result"] == "no-such-key":
            reply["result"] = "no-such-key"
        else:
            reply["result"] = "database-error"
        reply["error-message"] = message["error-message"]
        state["resilient-server"].send_reply(reply)
        return

    content_path = repository.content_path(
        message["avatar-id"], request_state.file_name
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
            error_string = "%s unlinking %s %s" % (
                state_key,
                content_path,
                instance
            )
            log.exception(error_string)
            reply["result"] = "exception"
            reply["error-message"] = error_string
            state["resilient-server"].send_reply(reply)
            return
  
    reply["result"] = "success"
    state["resilient-server"].send_reply(reply)

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
        "resilient-server"      : None,
        "pull-server"           : None,
        "database-client"       : None,
        "pub-server"            : None,
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
        for key, value in pickleable_state.items():
            state["active-requests"][key] = _request_state_tuple(**value)

    log.info("binding resilient-server to %s" % (_data_writer_address, ))
    state["resilient-server"] = ResilientServer(
        state["zmq-context"],
        _data_writer_address,
        state["receive-queue"]
    )
    state["resilient-server"].register(state["pollster"])

    log.info("binding pull-server to %s" % (_data_writer_pipeline_address, ))
    state["pull-server"] = PULLServer(
        state["zmq-context"],
        _data_writer_pipeline_address,
        state["receive-queue"]
    )
    state["pull-server"].register(state["pollster"])

    state["database-client"] = ResilientClient(
        state["zmq-context"],
        _database_server_local_address,
        _client_tag,
        _data_writer_pipeline_address
    )
    state["database-client"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    state["state-cleaner"] = StateCleaner(state)
    state["heartbeater"] = Heartbeater(
        state, 
        _heartbeat_interval,
        state["pub-server"]
    )

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
        (state["state-cleaner"].run, state["state-cleaner"].next_run(), ), 
        (state["heartbeater"].run, state["heartbeater"].next_run(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping resilient server")
    state["resilient-server"].close()

    log.debug("stopping database client")
    state["pull-server"].close()
    state["database-client"].close()

    log.debug("stopping (heartbeat) pub server")
    #state["pub-server"].close()

    state["zmq-context"].term()

    log.info("saving state")
    pickleable_state = dict()
    for state_key, request_state in state["active-requests"].items():
        pickleable_state[state_key] = request_state._asdict()

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


