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
import logging
import os
import sys
import time

from tools import amqp_connection
from tools.LRUCache import LRUCache
from tools import message_driven_process as process
from tools import repository
from tools.standard_logging import format_timestamp 

from diyapi_database_server import database_content

from messages.process_status import ProcessStatus

from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply
from messages.database_key_destroy import DatabaseKeyDestroy
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply
from messages.database_listmatch import DatabaseListMatch
from messages.database_listmatch_reply import DatabaseListMatchReply

_log_path = u"/var/log/pandora/diyapi_database_server_%s.log" % (
    os.environ["SPIDEROAK_MULTI_NODE_NAME"],
)
_queue_name = "database-server-%s" % (os.environ["SPIDEROAK_MULTI_NODE_NAME"], )
_routing_header = "database_server"
_routing_key_binding = ".".join([_routing_header, "*"])
_max_cached_databases = 10
_database_cache = "open-database-cache"
_max_listmatch_size = 1024 * 1024 * 1024

def _open_database(state, avatar_id):
    database = None

    if avatar_id in state[_database_cache]:
        database = state[_database_cache][avatar_id]
    else:
        database_path = repository.content_database_path(avatar_id)
        database = bsddb3.db.DB()
        database.set_flags(bsddb3.db.DB_DUPSORT)
        database.open(
            database_path, 
            dbtype=bsddb3.db.DB_BTREE, 
            flags=bsddb3.db.DB_CREATE 
        )
        state[_database_cache][avatar_id] = database

    return database

def _get_content(database, search_key, search_segment_number):
    """
    get a a database entry matching both key and segment number
    return None on failure
    """
    cursor = database.cursor()
    try:
        result = cursor.set(search_key)
        if result is None:
            return None
        key, content = result
        while database_content.segment_number(content) != search_segment_number:
            result = cursor.next_dup()
            if result is None:
                return None
            _, content = result
        return content
    finally:
        cursor.close()

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
        packed_existing_entry = _get_content(
            database, message.key, message.database_content.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        database.close()
        reply = DatabaseKeyInsertReply(
            message.request_id,
            DatabaseKeyInsertReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]
        
    previous_size = 0L
    if packed_existing_entry is not None \
    and database_content.segment_number(packed_existing_entry) == \
        message.database_content.segment_number:
        (existing_entry, _) = database_content.unmarshall(
            packed_existing_entry, 0
        )

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

        database.delete(message.key)

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
        packed_existing_entry = _get_content(
            database, message.key, message.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyLookupReply(
            message.request_id,
            DatabaseKeyLookupReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]
    finally:
        database.close()

    if packed_existing_entry is None:
        error_string = "unknown key: %s, %s %s" % (
            message.avatar_id, message.key, message.segment_number
        )
        log.warn(error_string)
        reply = DatabaseKeyLookupReply(
            message.request_id,
            DatabaseKeyLookupReply.error_unknown_key,
            error_message=error_string
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    (content, _) = database_content.unmarshall(packed_existing_entry, 0)
    reply = DatabaseKeyLookupReply(
        message.request_id,
        DatabaseKeyLookupReply.successful,
        database_content=content
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
        packed_existing_entry = _get_content(
            database, message.key, message.segment_number
        )
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyDestroyReply(
            message.request_id,
            DatabaseKeyDestroyReply.error_database_failure,
            error_message=str(instance)
        )
        database.close()
        return [(reply_exchange, reply_routing_key, reply, )]

    total_size = 0
    if packed_existing_entry is None:
        log.warn("%s no such key %s creating tombstone %s" % (
            message.avatar_id, message.key, format_timestamp(message.timestamp),
        ))
        content = database_content.create_tombstone(message.timestamp)
    else:
        (existing_entry, _) = database_content.unmarshall(
            packed_existing_entry, 0
        )
        # if the entry is already a tombstone, our mission is accomplished
        # but we want to make sure the tombstone has the newest date
        if existing_entry.is_tombstone:
            database.delete(message.key)
            content = existing_entry
            if message.timestamp > content.timestamp:
                log.debug("%s %s updating tombstone from %s to %s" % (
                    message.avatar_id, 
                    message.key, 
                    format_timestamp(content.timestamp),
                    format_timestamp(message.timestamp),
                ))
                content._update(timestamp=message.timestamp)
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
            database.delete(message.key)
            log.debug("%s %s creating tombstone %s total_size = %s" % (
                message.avatar_id, 
                message.key, 
                format_timestamp(message.timestamp),
                existing_entry.total_size
            ))
            total_size = existing_entry.total_size
            content = database_content.create_tombstone(message.timestamp)
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
    finally:
        database.close()

    reply = DatabaseKeyDestroyReply(
        message.request_id,
        DatabaseKeyDestroyReply.successful,
        total_size = total_size
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
            (key, _, ) = result
            if not key.startswith(message.prefix):
                break
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
    finally:
        database.close()

    reply = DatabaseListMatchReply(
        message.request_id,
        DatabaseListMatchReply.successful,
        is_complete,
        keys
    )
    return [(reply_exchange, reply_routing_key, reply, )]\

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

_dispatch_table = {
    DatabaseKeyInsert.routing_key   : _handle_key_insert,
    DatabaseKeyLookup.routing_key   : _handle_key_lookup,
    DatabaseKeyDestroy.routing_key  : _handle_key_destroy,
    DatabaseListMatch.routing_key   : _handle_listmatch,
    ProcessStatus.routing_key       : _handle_process_status,
}

def _startup(halt_event, state):
    message = ProcessStatus(
        time.time(),
        amqp_connection.local_exchange_name,
        _routing_header,
        ProcessStatus.status_startup
    )

    exchange = amqp_connection.broadcast_exchange_name
    routing_key = ProcessStatus.routing_key

    return [(exchange, routing_key, message, )]

def _shutdown(state):
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
    state = {_database_cache : LRUCache(_max_cached_databases)}
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state,
            pre_loop_function=_startup,
            in_loop_function=None,
            post_loop_function=_shutdown
        )
    )

