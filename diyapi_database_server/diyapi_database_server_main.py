# -*- coding: utf-8 -*-
"""
diyapi_database_server_main.py

Responds db key lookup requests (mostly from Data Reader)
Responds to db key insert requests (from Data Writer)
Responds to db key list requests from (web components)
Keeps LRU cache of databases open during normal operations.
Databases are simple  key/value stores. 
Every value either points to data or a tombstone and timestamp. 
Every data pointer includes
a timestamp, segment number, size of the segment, 
the combined size of the assembled segments and decoded segments, 
adler32 of the segment, 
and the md5 of the segment.
"""
import bsddb3.db
from collections import deque
import hashlib
import logging
import os
import os.path
import subprocess
import sys
import time

import zmq

from diyapi_tools.zeromq_pollster import ZeroMQPollster
from diyapi_tools.xrep_server import XREPServer
from diyapi_tools.deque_dispatcher import DequeDispatcher
from diyapi_tools import time_queue_driven_process
from diyapi_tools.LRUCache import LRUCache
from diyapi_tools import repository
from diyapi_tools.standard_logging import format_timestamp 

from diyapi_database_server import database_content

_local_node_name = os.environ["SPIDEROAK_MULTI_NODE_NAME"]
_log_path = u"/var/log/pandora/diyapi_database_server_%s.log" % (
    _local_node_name,
)
_database_server_address = os.environ.get(
    "DIYAPI_DATABASE_SERVER_ADDRESS",
    "ipc:///tmp/diyapi-database-server-%s/socket" % (_local_node_name, )
)
_max_cached_databases = 10
_database_cache = "open-database-cache"
_max_listmatch_size = 1024 * 1024 * 1024
_send_database_template = "/usr/bin/scp -q %s %s:%s/content.%s" 

def _content_database_path(state, avatar_id):
    """
    get the database path without creating the intervening directories
    if they do not exist
    """
    repository_path = os.environ["DIYAPI_REPOSITORY_PATH"]
    return state.get(
        "database-path", 
        os.path.join(
            repository_path, str(avatar_id), "contents.db"
        )
    )

def _content_database_exists(state, avatar_id):
    return os.path.exists(_content_database_path(state, avatar_id))

def _open_database(state, avatar_id):
    database = None

    if avatar_id in state[_database_cache]:
        database = state[_database_cache][avatar_id]
    else:
        # allow tests to specify a database path in state
        database_path = state.get(
            "database-path", repository.content_database_path(avatar_id)
        )
        database = bsddb3.db.DB()
        database.set_flags(bsddb3.db.DB_DUPSORT)
        database.open(
            database_path, 
            dbtype=bsddb3.db.DB_BTREE, 
            flags=bsddb3.db.DB_CREATE 
        )
        state[_database_cache][avatar_id] = database

    return database

def _get_content(
    database, search_key, search_version_number, search_segment_number
):
    """
    get a a database entry matching both key and segment number
    return None on failure
    """
    cursor = database.cursor()
    try:
        result = cursor.set(search_key)
        if result is None:
            return None
        _, packed_content = result
        (content, _) = database_content.unmarshall(packed_content, 0)
        while content.version_number != search_version_number \
        or content.segment_number != search_segment_number:
            result = cursor.next_dup()
            if result is None:
                return None
            _, packed_content = result
            (content, _) = database_content.unmarshall(packed_content, 0)
        return content
    finally:
        cursor.close()

def _get_content_any_segment(database, search_key, search_version_number):
    """
    get a a database entry matching key
    return None on failure
    """
    cursor = database.cursor()
    try:
        result = cursor.set(search_key)
        if result is None:
            return None
        _, packed_content = result
        (content, _) = database_content.unmarshall(packed_content, 0)
        while content.version_number != search_version_number:
            result = cursor.next_dup()
            if result is None:
                return None
            _, packed_content = result
            (content, _) = database_content.unmarshall(packed_content, 0)
        return content
    finally:
        cursor.close()

def _list_content(database, search_key):
    """
    get all database entry matching key (multiple segment numbers)
    return [] on failure
    """
    result_list = list()
    cursor = database.cursor()
    try:
        result = cursor.set(search_key)
        while result is not None:
            result_key, content = result
            assert result_key == search_key
            result_list.append(content)
            result = cursor.next_dup()
        return result_list
    finally:
        cursor.close()

def _find_avatars(state):
    """a generator to identify avatars with contents databases"""
    repository_path = os.environ["DIYAPI_REPOSITORY_PATH"]
    for file_name in os.listdir(repository_path):
        try:
            avatar_id = int(file_name)
        except ValueError:
            continue
        if _content_database_exists(state, avatar_id):
            yield avatar_id

def _handle_key_insert(state, message, _data):
    log = logging.getLogger("_handle_key_insert")
    content = database_content.factory(**message["database-content"])
    log.info("avatar_id = %s, key = %s version = %s segment = %s" % (
        message["avatar-id"], 
        str(message["key"]), 
        content.version_number,
        content.segment_number,
    ))

    reply = {
        "message-type"  : "key-insert-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "previous-size" : 0,
    }

    try:
        database = _open_database(state, message["avatar-id"])
        existing_entry = _get_content(
            database, 
            str(message["key"]), 
            content.version_number,
            content.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return
        
    previous_size = 0L
    if existing_entry is not None:

        # 2020-03-21 dougfort -- IRC conversation with Alan. we don't care
        # if it's a tombstone or not: an earlier timestamp is an error
        if content.timestamp < existing_entry.timestamp:
            error_string = "invalid duplicate %s < %s" % (
                format_timestamp(content.timestamp),
                format_timestamp(existing_entry.timestamp),
            )
            log.error(error_string)
            reply["result"] = "invalid-duplicate"
            reply["error-message"] = error_string
            state["xrep-server"].queue_message_for_send(reply)
            return

        if not existing_entry.is_tombstone:
            log.debug("found previous entry, size = %s" % (
                existing_entry.total_size,
            ))
            previous_size = existing_entry.total_size

        try:
            database.delete(str(message["key"]))
        except Exception, instance:
            log.exception("%s, %s" % (
                message["avatar-id"], str(message["key"]), 
            ))
            reply["result"] = "database-failure"
            reply["error-message"] = str(instance)
            state["xrep-server"].queue_message_for_send(reply)
            return

    try:
        database.put(str(message["key"]), database_content.marshall(content))
        database.sync()
        os.fsync(database.fd())
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    reply["previous-size"] = previous_size
    state["xrep-server"].queue_message_for_send(reply)

def _handle_key_lookup(state, message, _data):
    log = logging.getLogger("_handle_key_lookup")
    log.info("avatar_id = %s, key = %s version = %s segment = %s" % (
        message["avatar-id"], 
        str(message["key"]), 
        message["version-number"],
        message["segment-number"],
    ))

    reply = {
        "message-type"      : "key-lookup-reply",
        "xrep-ident"        : message["xrep-ident"],
        "request-id"        : message["request-id"],
        "result"            : None,
        "error-message"     : None,
        "database-content"  : None,
    }

    try:
        database = _open_database(state, message["avatar-id"])
        existing_entry = _get_content(
            database, 
            str(message["key"]), 
            message["version-number"],
            message["segment-number"]
        )
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    if existing_entry is None:
        error_string = "unknown key: %s, %s %s %s" % (
            message["avatar-id"], 
            str(message["key"]), 
            message["version-number"],
            message["segment-number"]
        )
        log.warn(error_string)
        reply["result"] = "unknown-key"
        reply["error-message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    reply["database-content"] = dict(existing_entry._asdict().items())
    state["xrep-server"].queue_message_for_send(reply)

def _handle_key_list(state, message, _data):
    log = logging.getLogger("_handle_key_list")
    log.info("avatar_id = %s, key = %s" % (
        message["avatar-id"], str(message["key"]), 
    ))

    reply = {
        "message-type"  : "key-list-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "content-list"  : list(),
    }

    try:
        database = _open_database(state, message["avatar-id"])
        packed_content_list = _list_content(database, str(message["key"]))
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    if len(packed_content_list) == 0:
        error_string = "unknown key: %s, %s" % (
            message["avatar-id"], str(message["key"])
        )
        log.warn(error_string)
        reply["result"] = "unknown-key"
        reply["error-message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    for packed_entry in packed_content_list:
        (content, _) = database_content.unmarshall(packed_entry, 0)
        if not content.is_tombstone:
            reply["content-list"].append(dict(content._asdict()))
    state["xrep-server"].queue_message_for_send(reply)

def _handle_key_destroy(state, message, _data):
    log = logging.getLogger("_handle_key_destroy")
    log.info("avatar_id = %s, key = %s version = %s segment = %s" % (
        message["avatar-id"], 
        str(message["key"]), 
        message["version-number"],
        message["segment-number"],
    ))

    reply = {
        "message-type"  : "key-destroy-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "total-size"    : 0,
    }

    try:
        database = _open_database(state, message["avatar-id"])
        existing_entry = _get_content(
            database, 
            str(message["key"]), 
            message["version-number"],
            message["segment-number"]
        )
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    total_size = 0
    delete_needed = False
    if existing_entry is None:
        log.warn("%s no such key %s creating tombstone %s" % (
            message["avatar-id"], 
            str(message["key"]), 
            format_timestamp(message["timestamp"]),
        ))
        content = database_content.create_tombstone(
            message["timestamp"], 
            message["version-number"], 
            message["segment-number"]
        )
    else:
        # if the entry is already a tombstone, our mission is accomplished
        # but we want to make sure the tombstone has the newest date
        if existing_entry.is_tombstone:
            delete_needed = True
            content = existing_entry
            if message["timestamp"] > content.timestamp:
                log.debug("%s %s updating tombstone from %s to %s" % (
                    message["avatar-id"], 
                    str(message["key"]), 
                    format_timestamp(content.timestamp),
                    format_timestamp(message["timestamp"]),
                ))
                content._replace(timestamp=message["timestamp"])
            else:
                log.debug("%s %s keeping existing tombstone %s > %s" % (
                    message["avatar-id"], 
                    str(message["key"]), 
                    format_timestamp(content.timestamp),
                    format_timestamp(message["timestamp"]),
                ))
        # if the timestamp on this message is newer than the existing entry
        # then we can overwrite
        elif message["timestamp"] > existing_entry.timestamp:
            delete_needed = True
            log.debug("%s %s creating tombstone %s total_size = %s" % (
                message["avatar-id"], 
                str(message["key"]), 
                format_timestamp(message["timestamp"]),
                existing_entry.total_size
            ))
            total_size = existing_entry.total_size
            content = database_content.create_tombstone(
                message["timestamp"],      
                message["version-number"],
                message["segment-number"]
            )
        # otherwise, we have an entry in the database that is newer than
        # this message, so we should not destroy
        else:
            log.warn("%s %s rejecting destroy %s > %s" % (
                message["avatar-id"], 
                str(message["key"]), 
                format_timestamp(existing_entry.timestamp),
                format_timestamp(message["timestamp"]),
            ))
            error_string = "%s %s database entry %s newer than destory %s" % (
                message["avatar-id"], 
                str(message["key"]), 
                format_timestamp(existing_entry.timestamp),
                format_timestamp(message["timestamp"]),
            )
            reply["result"] = "too-old"
            reply["error-message"] = error_string
            state["xrep-server"].queue_message_for_send(reply)
            return

    try:
        if delete_needed:
            database.delete(str(message["key"]))
        database.put(str(message["key"]), database_content.marshall(content))
        database.sync()
        os.fsync(database.fd())
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    reply["total-size"] = total_size
    state["xrep-server"].queue_message_for_send(reply)

def _handle_key_purge(state, message, _data):
    log = logging.getLogger("_handle_key_purge")
    log.info("avatar_id = %s, key = %s version = %s segment = %s" % (
        message["avatar-id"], 
        str(message["key"]), 
        message["version-number"],
        message["segment-number"],
    ))

    reply = {
        "message-type"  : "key-purge-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
    }

    try:
        database = _open_database(state, message["avatar-id"])
        existing_entry = _get_content(
            database, 
            str(message["key"]), 
            message["version-number"],
            message["segment-number"]
        )
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    if existing_entry is None:
        error_string = "%s no such key %s %s %s" % (
            message["avatar-id"], 
            str(message["key"]), 
            message["version-number"],
            message["segment-number"]
        )
        log.error(error_string)
        reply["result"] = "no-such-key"
        reply["error-message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return
    
    try:
        database.delete(str(message["key"]))
        database.sync()
        os.fsync(database.fd())
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply)

def _handle_listmatch(state, message, _data):
    log = logging.getLogger("_handle_listmatch")
    log.info("avatar_id = %s, prefix = %s" % (
        message["avatar-id"], message["prefix"], 
    ))

    reply = {
        "message-type"  : "key-purge-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "is-complete"   : None,
        "key-list"      : None,
    }

    keys = list()
    key_message_size = 0
    is_complete = True
    try:
        database = _open_database(state, message["avatar-id"])
        cursor = database.cursor()
        result = cursor.set_range(str(message["prefix"]))
        while result is not None:
            (key, packed_entry, ) = result
            if not key.startswith(message["prefix"]):
                break
            (content, _) = database_content.unmarshall(packed_entry, 0)
            if not content.is_tombstone:
                key_message_size += len(key)
                if key_message_size >  _max_listmatch_size:
                    is_complete = False
                    break
                keys.append(key)
            result = cursor.next()
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], message["prefix"], ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    reply["is-complete"] = is_complete
    reply["key-list"] = keys
    state["xrep-server"].queue_message_for_send(reply)

def _handle_consistency_check(state, message, _data):
    log = logging.getLogger("_handle_consistency_check")
    log.info("avatar_id = %s" % (message["avatar-id"], ))

    reply = {
        "message-type"  : "consistency-check-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "node-name"     : _local_node_name,
        "result"        : None,
        "error-message" : None,
        "md5-digest"    : None,
    }

    if not _content_database_exists(state, message["avatar-id"]):
        error_message = "no database for avatar_id %s" % (
            message["avatar-id"], 
        )
        log.error(error_message)
        reply["result"] = "database-failure"
        reply["error-message"] = error_message
        state["xrep-server"].queue_message_for_send(reply)
        return

    result_list = list()
    try:
        database = _open_database(state, message["avatar-id"])
        cursor = database.cursor()
        result = cursor.first()
        while result is not None:
            key, packed_content = result
            (content, _) = database_content.unmarshall(packed_content, 0)
            if content.timestamp < message["timestamp"]:
                if content.is_tombstone:
                    content_md5 = "tombstone"
                else:
                    content_md5 = content.file_md5
                result_list.append((key, content.timestamp, content_md5, ))
            result = cursor.next_dup()
    except Exception, instance:
        log.exception("%s" % (message["avatar-id"], ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return
        
    result_list.sort()
    md5 = hashlib.md5()
    for key, timestamp, content_md5 in result_list:
        md5.update(key)
        md5.update(str(timestamp))
        md5.update(content_md5)

    reply["result"] = "success"
    reply["md5-digest"] = md5.hexdigest()
    state["xrep-server"].queue_message_for_send(reply)

def _handle_avatar_database_request(state, message, _data):
    log = logging.getLogger("_handle_avatar_database_request")
    log.info("%s %s %s" % (
        message["avatar-id"], message["dest-host"], message["dest-dir"], 
    ))

    reply = {
        "message-type"  : "avatar-database-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "node-name"     : _local_node_name,
        "result"        : None,
        "error-message" : None,
    }

    if not _content_database_exists(state, message["avatar-id"]):
        error_message = "no database for avatar_id %s" % (
            message["avatar-id"], 
        )
        log.error(error_message)
        reply["result"] = "database-failure"
        reply["error-message"] = error_message
        state["xrep-server"].queue_message_for_send(reply)
        return

    send_database_command = _send_database_template % (
        _content_database_path(state, message["avatar-id"]),
        message["dest-host"],
        message["dest-dir"],
        _local_node_name
    )

    try:
        subprocess.check_call(args=send_database_command.split())
    except Exception, instance:
        log.exception(send_database_command)
        reply["result"] = "transmission-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    reply["result"] = "success"
    state["xrep-server"].queue_message_for_send(reply)

def _handle_avatar_list_request(state, message, _data):
    log = logging.getLogger("_handle_avatar_list_request")
    log.info("%s" % (message["request-id"], ))

    reply = {
        "message-type"  : "avatar-list-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "avatar-id-list": list(_find_avatars(state)),
    }
    state["xrep-server"].queue_message_for_send(reply)

def _handle_stat_request(state, message, _data):
    log = logging.getLogger("_handle_stat_request")
    log.info("avatar_id = %s" % (message["avatar-id"], ))

    reply = {
        "message-type"  : "stat-reply",
        "xrep-ident"    : message["xrep-ident"],
        "request-id"    : message["request-id"],
        "result"        : None,
        "error-message" : None,
        "timestamp"     : 0,
        "total_size"    : 0,
        "file_adler32"  : 0,
        "file_md5"      : 0,
        "userid"        : 0,
        "groupid"       : 0,
        "permissions"   : 0,
    }

    if not _content_database_exists(state, message["avatar-id"]):
        error_message = "no database for avatar_id %s" % (
            message["avatar-id"], 
        )
        log.error(error_message)
        reply["result"] = "database-failure"
        reply["error-message"] = error_message
        state["xrep-server"].queue_message_for_send(reply)
        return

    try:
        database = _open_database(state, message["avatar-id"])
        existing_entry = _get_content_any_segment(
            database, 
            str(message["key"]), 
            message["version-number"]
        )
    except Exception, instance:
        log.exception("%s, %s" % (message["avatar-id"], str(message["key"]), ))
        reply["result"] = "database-failure"
        reply["error-message"] = str(instance)
        state["xrep-server"].queue_message_for_send(reply)
        return

    if existing_entry is None:
        error_string = "%s no such key %s %s" % (
            message["avatar-id"], 
            str(message["key"]), 
            message["version-number"]
        )
        log.error(error_string)
        reply["result"] = "no-such-key"
        reply["error-message"] = error_string
        state["xrep-server"].queue_message_for_send(reply)
        return
    
    reply["result"] = "success"
    reply["timestamp"] = existing_entry.timestamp
    reply["total-size"] = existing_entry.total_size
    reply["file-adler32"] = existing_entry.file_adler32
    reply["file-md5"] = existing_entry.file_md5
    reply["userid"] = existing_entry.userid
    reply["groupid"] = existing_entry.groupid
    reply["permissions"] = existing_entry.permissions
    state["xrep-server"].queue_message_for_send(reply)

_dispatch_table = {
    "key-insert"                : _handle_key_insert,
    "key-lookup"                : _handle_key_lookup,
    "key-list"                  : _handle_key_list,
    "key-destroy"               : _handle_key_destroy,
    "key-purge"                 : _handle_key_purge,
    "listmatch"                 : _handle_listmatch,
    "consistency-check"         : _handle_consistency_check,
    "avatar-database-request"   : _handle_avatar_database_request,
    "avatar-list-request"       : _handle_avatar_list_request,
    "stat-request"              : _handle_stat_request,
}

def _create_state():
    return {
        _database_cache         : LRUCache(_max_cached_databases),
        "zmq-context"           : zmq.Context(),
        "pollster"              : ZeroMQPollster(),
        "xrep-server"           : None,
        "receive-queue"         : deque(),
        "queue-dispatcher"      : None,
    }

def _setup(_halt_event, _state):
    log = logging.getLogger("_setup")

    log.info("binding xrep-server to %s" % (_database_server_address, ))
    state["xrep-server"] = XREPServer(
        state["zmq-context"],
        _database_server_address,
        state["receive-queue"]
    )
    state["xrep-server"].register(state["pollster"])

    state["queue-dispatcher"] = DequeDispatcher(
        state,
        state["receive-queue"],
        _dispatch_table
    )

    # hand the pollster and the queue-dispatcher to the time-queue 
    return [
        (state["pollster"].run, time.time(), ), 
        (state["queue-dispatcher"].run, time.time(), ), 
    ] 

def _tear_down(_state):
    log = logging.getLogger("_tear_down")

    log.debug("stopping xrep server")
    state["xrep-server"].close()

    state["zmq-context"].term()
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

