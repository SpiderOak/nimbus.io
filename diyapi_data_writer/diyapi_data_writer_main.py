# -*- coding: utf-8 -*-
"""
diyapi_data_writer_main.py

Stores received segments (1 for each sequence) in the incoming directory with a temp extension.
When final segment is received
fsyncs temp data file
renames into place,
fsyncs the directory into which the file was renamed
sends message to the database server to record key as stored.
ACK back to to requestor includes size (from the database server) of any previous key 
this key supersedes (for space accounting.)
"""
import logging
import os
import sys
import time

from tools import message_driven_process as process
from tools import repository
from diyapi_database_server import database_content
from messages.archive_key_entire import ArchiveKeyEntire
from messages.database_key_insert import DatabaseKeyInsert
from messages.database_key_insert_reply import DatabaseKeyInsertReply

_log_path = u"/var/log/pandora/diyapi_data_writer.log"
_queue_name = "data_writer"
_routing_key_binding = "data_writer.*"

def _handle_archive_key_entire(state, message_body):
    log = logging.getLogger("_handle_archive_key_entire")
    message = ArchiveKeyEntire.unmarshall(message_body)
    log.info("avatar_id = %s, key = %s" % (message.avatar_id, message.key, ))


_dispatch_table = {
    ArchiveKeyEntire.routing_key : _handle_archive_key_entire,
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

