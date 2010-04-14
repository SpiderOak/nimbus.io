# -*- coding: utf-8 -*-
"""
forwarder_coroutine.py

a coroutine that handles message traffic for retrieving and
re-archiving a segment that was handed off to us
"""
import logging

from diyapi_tools import amqp_connection

from messages.retrieve_key_start import RetrieveKeyStart
from messages.retrieve_key_next import RetrieveKeyNext
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final import RetrieveKeyFinal
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next import ArchiveKeyNext
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final import ArchiveKeyFinal
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.purge_key import PurgeKey

def forwarder_coroutine(request_id, hint, reply_routing_header):
    """
    manage the message traffic for retrieving and re-archiving 
    a segment that was handed off to us
    """
    log = logging.getLogger("forwarder_coroutine")

    local_exchange = amqp_connection.local_exchange_name

    # start retrieving from our local data writer
    message = RetrieveKeyStart(
        request_id,
        hint.avatar_id,
        local_exchange,
        reply_routing_header,
        hint.key,
        hint.version_number,
        hint.segment_number
    )

    log.debug("yielding RetrieveKeyStart %s %s %s %s" % (
        hint.avatar_id, hint.key, hint.version_number, hint.segment_number
    ))
    
    retrieve_key_start_reply = \
        yield [(local_exchange, message.routing_key, message, )]

    assert retrieve_key_start_reply.result == 0

    segment_count = retrieve_key_start_reply.segment_count
    sequence = 0

    if retrieve_key_start_reply.segment_count == 1:
        message = ArchiveKeyEntire(
            request_id, 
            hint.avatar_id, 
            local_exchange,
            reply_routing_header,
            hint.timestamp, 
            hint.key, 
            hint.version_number,
            hint.segment_number, 
            retrieve_key_start_reply.adler32, 
            retrieve_key_start_reply.md5, 
            retrieve_key_start_reply.data_content
        )
    else:
        message = ArchiveKeyStart(
            request_id,
            hint.avatar_id, 
            local_exchange,
            reply_routing_header,
            hint.timestamp, 
            sequence, 
            hint.key, 
            hint.version_number, 
            hint.segment_number, 
            retrieve_key_start_reply.segment_size,
            retrieve_key_start_reply.data_content
        )
            
    archive_key_start_reply = \
        yield [(hint.exchange, message.routing_key, message, )]

    if archive_key_start_reply.__class__ == ArchiveKeyFinalReply:
        # tell our local data_writer to purge the key
        # and then we are done
        message = PurgeKey(
            request_id, 
            hint.avatar_id, 
            local_exchange,
            reply_routing_header,
            hint.timestamp, 
            hint.key, 
            hint.version_number,
            hint.segment_number 
        )
        yield [(local_exchange, message.routing_key, message, )]
        return 

    assert archive_key_start_reply.__class__ == ArchiveKeyStartReply

    sequence += 1

    # send the intermediate segments
    while sequence < segment_count-1:
        retrieve_messsage = RetrieveKeyNext(request_id, sequence)
        retrieve_reply = yield [
            (local_exchange, retrieve_messsage.routing_key, retrieve_messsage, )
        ]
        assert retrieve_reply.__class__ == RetrieveKeyNextReply
        assert retrieve_reply.result == 0
        archive_message = ArchiveKeyNext(
            request_id, sequence, retrieve_reply.data_content
        )
        archive_reply = yield [
            (hint.exchange, archive_message.routing_key, archive_message, )
        ]
        assert archive_reply.__class__ == ArchiveKeyNextReply
        assert archive_reply.result == 0

        sequence +=1

    # retrieve and archive the last segment
    retrieve_messsage = RetrieveKeyFinal(request_id, sequence)
    retrieve_reply = yield [
        (local_exchange, retrieve_messsage.routing_key, retrieve_messsage, )
    ]
    assert retrieve_reply.__class__ == RetrieveKeyFinalReply
    assert retrieve_reply.result == 0
    archive_message = ArchiveKeyFinal(
        request_id, 
        sequence,
        retrieve_key_start_reply.total_size,
        retrieve_key_start_reply.adler32,
        retrieve_key_start_reply.md5,
        retrieve_key_start_reply.data_content
    )
    archive_reply = yield [
        (hint.exchange, archive_message.routing_key, archive_message, )
    ]
    assert archive_reply.__class__ == ArchiveKeyFinalReply
    assert archive_reply.result == 0

    # tell our local data_writer to purge the key
    # and then we are done
    message = PurgeKey(
        request_id, 
        hint.avatar_id, 
        local_exchange,
        reply_routing_header,
        hint.timestamp, 
        hint.key, 
        hint.version_number,
        hint.segment_number 
    )
    yield [(local_exchange, message.routing_key, message, )]

