# -*- coding: utf-8 -*-
"""
forwarder_coroutine.py

a coroutine that handles message traffic for retrieving and
re-archiving a segment that was handed off to us
"""
import logging

from diyapi_tools import amqp_connection

from messages.retrieve_key_start import RetrieveKeyStart
from messages.archive_key_start import ArchiveKeyStart
from messages.archive_key_entire import ArchiveKeyEntire

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

    
