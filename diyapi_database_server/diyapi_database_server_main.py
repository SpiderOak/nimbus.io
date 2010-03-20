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

from tools import message_driven_process as process
from tools import repository
from diyapi_database_server import database_content
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

_log_path = u"/var/log/pandora/diyapi_database_server.log"
_queue_name = "database_server"
_routing_key_binding = "database_server.*"

def _open_database(avatar_id):
    database_path = repository.content_database_path(avatar_id)
    database = bsddb3.db.DB()
    database.open(
        database_path, dbtype=bsddb3.db.DB_HASH, flags=bsddb3.db.DB_CREATE
    )
    return database

def _handle_key_insert(state, message_body):
    log = logging.getLogger("_handle_key_insert")
    message = DatabaseKeyInsert.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))

    database = _open_database(message.avatar_id)

    previous_size = 0L
    reply = None
    if database.exists(message.key):
        existing_entry = database_content.factory._make(database,get(key))
        if not existing_entry.is_tombstone:
            if message.content.timestamp < existing_entry.timestamp:
                error_string = "invalid duplicate %s < %s" % (
                    time.asctime(message.content.timestamp),
                    time.asctime(existing_entry.timestamp),
                )
                log.error(error_string)
                reply = DatabaseKeyInsertReply(
                    message.request_id,
                    DatabaseKeyInsertReply.error_invalid_duplicate,
                    error_message=error_string
                )
            else:
                log.debug("found previous entry, size = %s" % (
                    entry.total_size,
                ))
                previous_size = entry.total_size

    if reply is None: # no error message so far
        database.put(message.key, database_content.marshall(message.content))
        reply = DatabaseKeyInsertReply(
            message.request_id,
            DatabaseKeyInsertReply.successful,
            previous_size,
        )

    return [(message.reply_exchange, message.reply_routing_key, reply, )]

_dispatch_table = {
    DatabaseKeyInsert.routing_key : _handle_key_insert
}

if __name__ == "__main__":
    sys.exit(
        process.main(
            _log_path, _queue_name, _routing_key_binding, _dispatch_table
        )
    )

