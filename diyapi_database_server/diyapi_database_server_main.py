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

from tools.LRUCache import LRUCache
from tools import message_driven_process as process
from tools import repository
from diyapi_database_server import database_content
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply

_log_path = u"/var/log/pandora/diyapi_database_server.log"
_queue_name = "database_server"
_routing_key_binding = "database_server.*"
_max_cached_databases = 10
_database_cache = "open-database-cache"

def _open_database(state, avatar_id):
    database = None

    if avatar_id in state[_database_cache]:
        database = state[_database_cache][avatar_id]
    else:
        database_path = repository.content_database_path(avatar_id)
        database = bsddb3.db.DB()
        database.open(
            database_path, dbtype=bsddb3.db.DB_BTREE, flags=bsddb3.db.DB_CREATE
        )
        state[_database_cache][avatar_id] = database

    return database

def _handle_key_insert(state, message_body):
    log = logging.getLogger("_handle_key_insert")
    message = DatabaseKeyInsert.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = message.reply_routing_key

    try:
        database = _open_database(state, message.avatar_id)
        packed_existing_entry = database.get(message.key)
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyInsertReply(
            message.request_id,
            DatabaseKeyInsertReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]
        
    previous_size = 0L
    if packed_existing_entry is not None:
        (existing_entry, _) = database_content.unmarshall(
            packed_existing_entry, 0
        )

        # 2020-03-21 dougfort -- IRC conversation with Alan. we don't care
        # if it's a tombstone or not: an earlier timestamp is an error
        if message.database_content.timestamp < existing_entry.timestamp:
            error_string = "invalid duplicate %s < %s" % (
                time.asctime(time.localtime(
                    message.database_content.timestamp)
                ),
                time.asctime(time.localtime(existing_entry.timestamp)),
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
    log = logging.getLogger("_handle_key_insert")
    message = DatabaseKeyLookup.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    reply_exchange = message.reply_exchange
    reply_routing_key = message.reply_routing_key

    try:
        database = _open_database(state, message.avatar_id)
        packed_existing_entry = database.get(message.key)
    except Exception, instance:
        log.exception("%s, %s" % (message.avatar_id, message.key, ))
        reply = DatabaseKeyLookupReply(
            message.request_id,
            DatabaseKeyLookupReply.error_database_failure,
            error_message=str(instance)
        )
        return [(reply_exchange, reply_routing_key, reply, )]

    (content, _) = database_content.unmarshall(packed_existing_entry, 0)
    reply = DatabaseKeyLookupReply(
        message.request_id,
        DatabaseKeyLookupReply.successful,
        database_content=content
    )
    return [(reply_exchange, reply_routing_key, reply, )]

_dispatch_table = {
    DatabaseKeyInsert.routing_key : _handle_key_insert,
    DatabaseKeyLookup.routing_key : _handle_key_lookup,
}

if __name__ == "__main__":
    state = {_database_cache : LRUCache(_max_cached_databases)}
    sys.exit(
        process.main(
            _log_path, 
            _queue_name, 
            _routing_key_binding, 
            _dispatch_table, 
            state
        )
    )

