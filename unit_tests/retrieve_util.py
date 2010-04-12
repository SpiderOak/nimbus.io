# -*- coding: utf-8 -*-
"""
retrieve_util.py

retrieve utility for testing
"""

from diyapi_tools import amqp_connection

from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.database_key_lookup import DatabaseKeyLookup

from diyapi_data_reader.diyapi_data_reader_main import \
        _handle_retrieve_key_start, \
        _handle_retrieve_key_next, \
        _handle_retrieve_key_final, \
        _handle_key_lookup_reply
from diyapi_database_server.diyapi_database_server_main import \
        _handle_key_lookup

def retrieve_coroutine(self, data_reader_state, database_state, start_message):
    """retrieve content for a key"""
    marshalled_message = start_message.marshall()

    replies = _handle_retrieve_key_start(
        data_reader_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)

    # we expect the data reader to send a
    # database_key_lookup to the database server
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply_exchange, amqp_connection.local_exchange_name)
    self.assertEqual(reply_routing_key, DatabaseKeyLookup.routing_key)
    self.assertEqual(reply.__class__, DatabaseKeyLookup)
    self.assertEqual(reply.request_id, start_message.request_id)

    # hand off the reply to the database server
    marshalled_message = reply.marshall()
    replies = _handle_key_lookup(database_state, marshalled_message)
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.request_id, start_message.request_id)
    self.assertEqual(reply.result, 0)

    # pass the database server reply back to data_reader
    # we should get a reply we can send to the web api 
    marshalled_message = reply.marshall()
    replies = _handle_key_lookup_reply(
        data_reader_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.__class__, RetrieveKeyStartReply)

    segment_count = reply.segment_count

    # we have sequence 0, get sequence 1..N-1
    for _ in range(1, segment_count-1):
        message = yield reply
        marshalled_message = message.marshall()
        replies = _handle_retrieve_key_next(
            data_reader_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, RetrieveKeyNextReply)

    # get the last segment
    message = yield reply
    marshalled_message = message.marshall()
    replies = _handle_retrieve_key_final(
        data_reader_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.__class__, RetrieveKeyFinalReply)
    yield reply

