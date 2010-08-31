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
import hashlib
import logging
import os
import os.path
import subprocess
import sys
import time

from diyapi_tools import amqp_connection
from diyapi_tools.LRUCache import LRUCache
from diyapi_tools import message_driven_process as process
from diyapi_tools import repository
from diyapi_tools.standard_logging import format_timestamp 

from diyapi_database_server import database_content

from messages.process_status import ProcessStatus

from messages.database_avatar_database_request import \
    DatabaseAvatarDatabaseRequest
from messages.database_avatar_database_reply import DatabaseAvatarDatabaseReply
from messages.database_avatar_list_request import DatabaseAvatarListRequest
from messages.database_avatar_list_reply import DatabaseAvatarListReply
from messages.database_consistency_check import DatabaseConsistencyCheck
from messages.database_consistency_check_reply import \
    DatabaseConsistencyCheckReply
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply
from messages.database_key_list import DatabaseKeyList
from messages.database_key_list_reply import DatabaseKeyListReply
from messages.database_key_destroy import DatabaseKeyDestroy
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply
from messages.database_key_purge import DatabaseKeyPurge
from messages.database_key_purge_reply import DatabaseKeyPurgeReply
from messages.database_listmatch import DatabaseListMatch
from messages.database_listmatch_reply import DatabaseListMatchReply
from messages.stat import Stat
from messages.stat_reply import StatReply

_routing_header = "database_server"
_routing_key_binding = ".".join([_routing_header, "*"])
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

def _handle_key_insert(state, message_body):
    log = logging.getLogger("_handle_key_insert")
    message = DatabaseKeyInsert.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DatabaseKeyInsertReply.routing_tag]
    )

    try:
        database = _open_database(state, message.avatar_id)
        existing_entry = _get_content(
            database, 
            message.key, 
            message.database_content.version_number,
            message.database_content.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyInsertReply(
            message.request_id,
            DatabaseKeyInsertReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]
        
    previous_size = 0L
    if existing_entry is not None:

        # 2020-03-21 dougfort -- IRC conversation with Alan. we don't care
        # if it's a tombstone or not: an earlier timestamp is an error
        if message.database_content.timestamp < existing_entry.timestamp:
            error_string = "invalid duplicate %s < %s" % (
                format_timestamp(message.database_content.timestamp),
                format_timestamp(existing_entry.timestamp),
            )
            log.error(error_string)
            reply = DatabaseKeyInsertReply(
                message.request_id,
                DatabaseKeyInsertReply.error_invalid_duplicate,
                error_message=error_string
            )
            return [(reply_exchange, reply_routing_key, reply, )]

        if not existing_entry.is_tombstone:
            log.debug("found previous entry, size = %s" % (
                existing_entry.total_size,
            ))
            previous_size = existing_entry.total_size

        try:
            database.delete(message.key)
        except Exception, instance:
            log.exception("%s, %s" % (message.avatar_id, message.key, ))
            reply = DatabaseKeyInsertReply(
                message.request_id,
                DatabaseKeyInsertReply.error_database_failure,
                error_message=str(instance)
            )
            return [(reply_exchange, reply_routing_key, reply, )]

    try:
        database.put(
            message.key, database_content.marshall(message.database_content)
        )
        database.sync()
        os.fsync(database.fd())
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyInsertReply(
            message.request_id,
            DatabaseKeyInsertReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = DatabaseKeyInsertReply(
        message.request_id,
        DatabaseKeyInsertReply.successful,
        previous_size,
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_key_lookup(state, message_body):
    log = logging.getLogger("_handle_key_lookup")
    message = DatabaseKeyLookup.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DatabaseKeyLookupReply.routing_tag]
    )

    try:
        database = _open_database(state, message.avatar_id)
        existing_entry = _get_content(
            database, 
            message.key, 
            message.version_number,
            message.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyLookupReply(
            message.request_id,
            DatabaseKeyLookupReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    if existing_entry is None:
        error_string = "unknown key: %s, %s %s %s" % (
            message.avatar_id, 
            message.key, 
            message.version_number,
            message.segment_number
        )
        log.warn(error_string)
        reply = DatabaseKeyLookupReply(
            message.request_id,
            DatabaseKeyLookupReply.error_unknown_key,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = DatabaseKeyLookupReply(
        message.request_id,
        DatabaseKeyLookupReply.successful,
        database_content=existing_entry
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_key_list(state, message_body):
    log = logging.getLogger("_handle_key_list")
    message = DatabaseKeyList.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DatabaseKeyListReply.routing_tag]
    )

    try:
        database = _open_database(state, message.avatar_id)
        packed_content_list = _list_content(database, message.key)
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyListReply(
            message.request_id,
            DatabaseKeyListReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    if len(packed_content_list) == 0:
        error_string = "unknown key: %s, %s" % (
            message.avatar_id, message.key
        )
        log.warn(error_string)
        reply = DatabaseKeyListReply(
            message.request_id,
            DatabaseKeyListReply.error_unknown_key,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    content_list = []
    for packed_entry in packed_content_list:
        (content, _) = database_content.unmarshall(packed_entry, 0)
        if not content.is_tombstone:
            content_list.append(content)

    reply = DatabaseKeyListReply(
        message.request_id,
        DatabaseKeyListReply.successful,
        content_list=content_list
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_key_destroy(state, message_body):
    log = logging.getLogger("_handle_key_destroy")
    message = DatabaseKeyDestroy.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DatabaseKeyDestroyReply.routing_tag]
    )

    try:
        database = _open_database(state, message.avatar_id)
        existing_entry = _get_content(
            database, 
            message.key, 
            message.version_number,
            message.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyDestroyReply(
            message.request_id,
            DatabaseKeyDestroyReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    total_size = 0
    delete_needed = False
    if existing_entry is None:
        log.warn("%s no such key %s creating tombstone %s" % (
            message.avatar_id, message.key, format_timestamp(message.timestamp),
        ))
        content = database_content.create_tombstone(
            message.timestamp, message.version_number, message.segment_number
        )
    else:
        # if the entry is already a tombstone, our mission is accomplished
        # but we want to make sure the tombstone has the newest date
        if existing_entry.is_tombstone:
            delete_needed = True
            content = existing_entry
            if message.timestamp > content.timestamp:
                log.debug("%s %s updating tombstone from %s to %s" % (
                    message.avatar_id, 
                    message.key, 
                    format_timestamp(content.timestamp),
                    format_timestamp(message.timestamp),
                ))
                content._replace(timestamp=message.timestamp)
            else:
                log.debug("%s %s keeping existing tombstone %s > %s" % (
                    message.avatar_id, 
                    message.key, 
                    format_timestamp(content.timestamp),
                    format_timestamp(message.timestamp),
                ))
        # if the timestamp on this message is newer than the existing entry
        # then we can overwrite
        elif message.timestamp > existing_entry.timestamp:
            delete_needed = True
            log.debug("%s %s creating tombstone %s total_size = %s" % (
                message.avatar_id, 
                message.key, 
                format_timestamp(message.timestamp),
                existing_entry.total_size
            ))
            total_size = existing_entry.total_size
            content = database_content.create_tombstone(
                message.timestamp,      
                message.version_number,
                message.segment_number
            )
        # otherwise, we have an entry in the database that is newer than
        # this message, so we should not destroy
        else:
            log.warn("%s %s rejecting destroy %s > %s" % (
                message.avatar_id, 
                message.key, 
                format_timestamp(existing_entry.timestamp),
                format_timestamp(message.timestamp),
            ))
            error_string = "%s %s database entry %s newer than destory %s" % (
                message.avatar_id, 
                message.key, 
                format_timestamp(existing_entry.timestamp),
                format_timestamp(message.timestamp),
            )
            reply = DatabaseKeyDestroyReply(
                message.request_id,
                DatabaseKeyDestroyReply.error_too_old,
                error_message=error_string
            )
            return [(reply_exchange, reply_routing_key, reply, )]

    try:
        if delete_needed:
            database.delete(message.key)
        database.put(message.key, database_content.marshall(content))
        database.sync()
        os.fsync(database.fd())
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyDestroyReply(
            message.request_id,
            DatabaseKeyDestroyReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = DatabaseKeyDestroyReply(
        message.request_id,
        DatabaseKeyDestroyReply.successful,
        total_size = total_size
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_key_purge(state, message_body):
    log = logging.getLogger("_handle_key_purge")
    message = DatabaseKeyPurge.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DatabaseKeyPurgeReply.routing_tag]
    )

    try:
        database = _open_database(state, message.avatar_id)
        existing_entry = _get_content(
            database, 
            message.key, 
            message.version_number,
            message.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyPurgeReply(
            message.request_id,
            DatabaseKeyPurgeReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    if existing_entry is None:
        error_string = "%s no such key %s %s %s" % (
            message.avatar_id, 
            message.key, 
            message.version_number,
            message.segment_number
        )
        log.error(error_string)
        reply = DatabaseKeyPurgeReply(
            message.request_id,
            DatabaseKeyPurgeReply.error_key_not_found,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )]
    
    try:
        database.delete(message.key)
        database.sync()
        os.fsync(database.fd())
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyPurgeReply(
            message.request_id,
            DatabaseKeyPurgeReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = DatabaseKeyPurgeReply(
        message.request_id,
        DatabaseKeyPurgeReply.successful
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_listmatch(state, message_body):
    log = logging.getLogger("_handle_listmatch")
    message = DatabaseListMatch.unmarshall(message_body)
    log.info("avatar_id = %s, prefix = %s" % (
        message.avatar_id, message.prefix, 
    ))

    reply_exchange = message.reply_exchange
    reply_routing_key = "".join(
        [message.reply_routing_header, ".", DatabaseListMatchReply.routing_tag]
    )

    keys = list()
    key_message_size = 0
    is_complete = True
    try:
        database = _open_database(state, message.avatar_id)
        cursor = database.cursor()
        result = cursor.set_range(message.prefix)
        while result is not None:
            (key, packed_entry, ) = result
            if not key.startswith(message.prefix):
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
        log.exception("%s, %s" % (message.avatar_id, message.prefix, ))
        reply = DatabaseListMatchReply(
            message.request_id,
            DatabaseListMatchReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = DatabaseListMatchReply(
        message.request_id,
        DatabaseListMatchReply.successful,
        is_complete,
        keys
    )
    return [(reply_exchange, reply_routing_key, reply, )]\

def _handle_consistency_check(state, message_body):
    log = logging.getLogger("_handle_consistency_check")
    message = DatabaseConsistencyCheck.unmarshall(message_body)
    log.info("avatar_id = %s" % (message.avatar_id, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = ".".join([
        message.reply_routing_header, DatabaseConsistencyCheckReply.routing_tag
    ])

    if not _content_database_exists(state, message.avatar_id):
        error_message = "no database for avatar_id %s" % (message.avatar_id, )
        log.error(error_message)
        reply = DatabaseConsistencyCheckReply(
            message.request_id,
            state["node-name"],
            DatabaseConsistencyCheckReply.error_database_failure,
            error_message=error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    result_list = list()
    try:
        database = _open_database(state, message.avatar_id)
        cursor = database.cursor()
        result = cursor.first()
        while result is not None:
            key, packed_content = result
            (content, _) = database_content.unmarshall(packed_content, 0)
            if content.timestamp < message.timestamp:
                if content.is_tombstone:
                    content_md5 = "tombstone"
                else:
                    content_md5 = content.file_md5
                result_list.append((key, content.timestamp, content_md5, ))
            result = cursor.next_dup()
    except Exception, instance:
        log.exception("%s" % (message.avatar_id, ))
        reply = DatabaseConsistencyCheckReply(
            message.request_id,
            state["node-name"],
            DatabaseConsistencyCheckReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]
        
    result_list.sort()
    md5 = hashlib.md5()
    for key, timestamp, content_md5 in result_list:
        md5.update(key)
        md5.update(str(timestamp))
        md5.update(content_md5)

    reply = DatabaseConsistencyCheckReply(
        message.request_id,
        state["node-name"],
        DatabaseConsistencyCheckReply.successful,
        hash_value=md5.hexdigest()
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_avatar_database_request(state, message_body):
    log = logging.getLogger("_handle_avatar_database_request")
    message = DatabaseAvatarDatabaseRequest.unmarshall(message_body)
    log.info("reply exchange = %s" % (message.reply_exchange, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = ".".join([
        message.reply_routing_header, DatabaseAvatarDatabaseReply.routing_tag
    ])

    if not _content_database_exists(state, message.avatar_id):
        error_message = "no database for avatar_id %s" % (message.avatar_id, )
        log.error(error_message)
        reply = DatabaseAvatarDatabaseReply(
            message.request_id,
            state["node-name"],
            DatabaseAvatarDatabaseReply.error_database_failure,
            error_message=error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    send_database_command = _send_database_template % (
        _content_database_path(state, message.avatar_id),
        message.dest_host,
        message.dest_dir,
        state["node-name"]
    )

    try:
        subprocess.check_call(args=send_database_command.split())
    except Exception, instance:
        log.exception(send_database_command)
        reply = DatabaseAvatarDatabaseReply(
            message.request_id,
            state["node-name"],
            DatabaseAvatarDatabaseReply.error_transmission_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    reply = DatabaseAvatarDatabaseReply(
        message.request_id,
        state["node-name"], 
        DatabaseAvatarDatabaseReply.successful
    )
    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_avatar_list_request(state, message_body):
    log = logging.getLogger("_handle_avatar_list_request")
    message = DatabaseAvatarListRequest.unmarshall(message_body)
    log.info("reply exchange = %s" % (message.reply_exchange, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = ".".join([
        message.reply_routing_header, DatabaseAvatarListReply.routing_tag
    ])
    reply = DatabaseAvatarListReply(message.request_id)
    reply.put(_find_avatars(state))

    return [(reply_exchange, reply_routing_key, reply, )]

def _handle_process_status(_state, message_body):
    log = logging.getLogger("_handle_process_status")
    message = ProcessStatus.unmarshall(message_body)
    log.debug("%s %s %s %s" % (
        message.exchange,
        message.routing_header,
        message.status,
        format_timestamp(message.timestamp),
    ))
    return []

def _handle_stat_request(state, message_body):
    log = logging.getLogger("_handle_stat_request")
    message = Stat.unmarshall(message_body)
    log.info("avatar_id = %s" % (message.avatar_id, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = ".".join([
        message.reply_routing_header, StatReply.routing_tag
    ])

    if not _content_database_exists(state, message.avatar_id):
        error_message = "no database for avatar_id %s" % (message.avatar_id, )
        log.error(error_message)
        reply = StatReply(
            message.request_id,
            StatReply.error_database_failure,
            error_message=error_message
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    try:
        database = _open_database(state, message.avatar_id)
        existing_entry = _get_content_any_segment(
            database, 
            message.key, 
            message.version_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = StatReply(
            message.request_id,
            StatReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    if existing_entry is None:
        error_string = "%s no such key %s %s" % (
            message.avatar_id, 
            message.key, 
            message.version_number
        )
        log.error(error_string)
        reply = StatReply(
            message.request_id,
            StatReply.error_key_not_found,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )]
    
    reply = StatReply(
        message.request_id,
        StatReply.successful,
        total_size=existing_entry.total_size,
        timestamp=existing_entry.timestamp,
        file_adler32=existing_entry.file_adler32,
        file_md5=existing_entry.file_md5,
        userid=existing_entry.userid,
        groupid=existing_entry.groupid,
        permissions=existing_entry.permissions,
    )
    return [(reply_exchange, reply_routing_key, reply, )]

_dispatch_table = {
    DatabaseKeyInsert.routing_key           : _handle_key_insert,
    DatabaseKeyLookup.routing_key           : _handle_key_lookup,
    DatabaseKeyList.routing_key             : _handle_key_list,
    DatabaseKeyDestroy.routing_key          : _handle_key_destroy,
    DatabaseKeyPurge.routing_key            : _handle_key_purge,
    DatabaseListMatch.routing_key           : _handle_listmatch,
    DatabaseConsistencyCheck.routing_key    : _handle_consistency_check,
    DatabaseAvatarDatabaseRequest.routing_key : \
        _handle_avatar_database_request,
    DatabaseAvatarListRequest.routing_key   : _handle_avatar_list_request,
    Stat.routing_key                        : _handle_stat_request,
    ProcessStatus.routing_key               : _handle_process_status,
}

def _startup(_halt_event, _state):
    message = ProcessStatus(
        time.time(),
        amqp_connection.local_exchange_name,
        _routing_header,
        ProcessStatus.status_startup
    )

    exchange = amqp_connection.broadcast_exchange_name
    routing_key = ProcessStatus.routing_key

    return [(exchange, routing_key, message, )]

def _shutdown(_state):
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
    state = {
        _database_cache : LRUCache(_max_cached_databases),
        "node-name"     : os.environ["SPIDEROAK_MULTI_NODE_NAME"]
    }
    log_path = u"/var/log/pandora/diyapi_database_server_%s.log" % ( 
        state["node-name"],
    )
    queue_name = "database-server-%s" % (state["node-name"], )
    sys.exit(
        process.main(
            log_path, 
            queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_startup,
            in_loop_function=None,
            post_loop_function=_shutdown
        )
    )

