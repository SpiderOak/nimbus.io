# -*- coding: utf-8 -*-
"""
retrieve_util.py

retrieve utility for testing
"""
import logging
import uuid

from diyapi_tools import amqp_connection

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.database_key_lookup import DatabaseKeyLookup
from messages.database_key_lookup_reply import DatabaseKeyLookupReply

from diyapi_data_reader.diyapi_data_reader_main import \
        _handle_retrieve_key_start, \
        _handle_retrieve_key_next, \
        _handle_retrieve_key_final, \
        _handle_key_lookup_reply
from diyapi_database_server.diyapi_database_server_main import \
        _database_cache, _handle_key_lookup
_reply_routing_header = "test_data_reader"

def retrieve_generator(self, avatar_id, key, version_number, segment_number):
    """retrieve content for a key"""
    request_id = uuid.uuid1().hex
    test_exchange = "reply-exchange"
    message = RetrieveKeyStart(
        request_id,
        avatar_id,
        test_exchange,
        _reply_routing_header,
        key,
        version_number,
        segment_number
    )
    marshalled_message = message.marshall()

    data_reader_state = dict()
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
    self.assertEqual(reply.request_id, request_id)

    # hand off the reply to the database server
    marshalled_message = reply.marshall()
    database_state = {_database_cache : dict()}
    replies = _handle_key_lookup(database_state, marshalled_message)
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.request_id, request_id)
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
    self.assertEqual(reply.result, 0)

    segment_count = reply.segment_count
    yield reply.data_content

    # if the file only has one segment, we are done
    if  segment_count == 1:
        return

    # we have segment 0, get segments 1..N-1
    for sequence in range(1, segment_count-1):
        message = RetrieveKeyNext(request_id, sequence)
        marshalled_message = message.marshall()
        replies = _handle_retrieve_key_next(
            data_reader_state, marshalled_message
        )
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply.__class__, RetrieveKeyNextReply)
        self.assertEqual(reply.result, 0)
        yield reply.data_content

    # get the last segment
    sequence = segment_count - 1
    message = RetrieveKeyFinal(request_id, sequence)
    marshalled_message = message.marshall()
    replies = _handle_retrieve_key_final(
        data_reader_state, marshalled_message
    )
    self.assertEqual(len(replies), 1)
    [(reply_exchange, reply_routing_key, reply, ), ] = replies
    self.assertEqual(reply.__class__, RetrieveKeyFinalReply)
    self.assertEqual(reply.result, 0)
    yield reply.data_content

