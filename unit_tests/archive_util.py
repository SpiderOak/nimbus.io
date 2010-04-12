# -*- coding: utf-8 -*-
"""
archive_util.py

This is a form of mixin, extracting common code from test_data_writer and
test_data_reader
"""
from diyapi_tools import amqp_connection

from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_next import ArchiveKeyNext
from messages.database_key_insert import DatabaseKeyInsert

from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_insert
from diyapi_data_writer.diyapi_data_writer_main import \
        _handle_archive_key_entire, \
        _handle_key_insert_reply, \
        _handle_archive_key_start, \
        _handle_archive_key_next, \
        _handle_archive_key_final

_reply_routing_header = "test_archive"

def archive_coroutine(self, start_message):
    marshalled_message = start_message.marshall()

    data_writer_state = dict()

    if start_message.__class__ == ArchiveKeyEntire:
        replies = _handle_archive_key_entire(
            data_writer_state, marshalled_message
        )
    else:
        replies = _handle_archive_key_start(
            data_writer_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)

        # we should get a successful reply 
        [(reply_exchange, reply_routing_key, reply, ), ] = replies

        message = yield reply

        # do the interior content
        while message.__class__ == ArchiveKeyNext:

            marshalled_message = message.marshall()

            marshalled_message = message.marshall()

            replies = _handle_archive_key_next(
                data_writer_state, marshalled_message
            )
            self.assertEqual(len(replies), 1)

            [(reply_exchange, reply_routing_key, reply, ), ] = replies

            message = yield reply

        # send the last one
        marshalled_message = message.marshall()

        replies = _handle_archive_key_final(
            data_writer_state, marshalled_message
        )

    self.assertEqual(len(replies), 1)

    # after a successful write, we expect the data writer to send a
    # database_key_insert to the database server
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
    self.assertEqual(reply_routing_key, DatabaseKeyInsert.routing_key)
    self.assertEqual(reply.__class__, DatabaseKeyInsert)
    self.assertEqual(reply.request_id, start_message.request_id)

    # hand off the reply to the database server
    marshalled_message = reply.marshall()
    database_state = {_database_cache : dict()}
    replies = _handle_key_insert(database_state, marshalled_message)
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.request_id, start_message.request_id)
    self.assertEqual(reply.result, 0)
    self.assertEqual(reply.previous_size, 0)

    # pass the database server reply back to data_writer
    # we should get a reply we can send to the web api 
    marshalled_message = reply.marshall()
    replies = _handle_key_insert_reply(
        data_writer_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies

    yield reply

