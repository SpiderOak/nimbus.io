# -*- coding: utf-8 -*-
"""
forwarder_coroutine.py

a coroutine that handles message traffic for retrieving and
re-archiving a segment that was handed off to us
"""
import logging
import time
import uuid

def forwarder_coroutine(
    hint, writer_client, reader_client, backup_writer_clients
):
    """
    manage the message traffic for retrieving and re-archiving 
    a segment that was handed off to us
    """
    log = logging.getLogger("forwarder_coroutine")

    # start retrieving from our reader
    message_id = uuid.uuid1().hex
    message = {
        "message-type"      : "retrieve-key-start",
        "message-id"        : message_id,
        "avatar-id"         : hint.avatar_id,
        "key"               : hint.key,
        "version-number"    : hint.version_number,
        "segment-number"    : hint.segment_number,
    }

    log.debug("sending retrieve-key-start %s %s %s %s" % (
        hint.avatar_id, hint.key, hint.version_number, hint.segment_number
    ))
    
    reader_client.queue_message_for_send(message, data=None)
    reply, data = yield message_id

    assert reply["message-type"] == "retrieve-key-start-reply", reply
    assert reply["result"] == "success", reply

    # note that we expect the md5 digests to be base64 encoded 
    # (zeromq's json likes them like that)
    # we don't decode, because we're send them right back out again

    total_size      = reply["total-size"]
    file_adler32    = reply["file-adler32"]
    file_md5        = reply["file-md5"]
    segment_adler32 = reply["segment-adler32"]
    segment_md5     = reply["segment-md5"]
    segment_count   = reply["segment-count"]

    sequence = 0

    message_id = uuid.uuid1().hex
    if reply["segment-count"] == 1:
        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "timestamp"         : hint.timestamp,
            "key"               : hint.key, 
            "version-number"    : hint.version_number,
            "segment-number"    : hint.segment_number,
            "total-size"        : total_size,
            "file-adler32"      : file_adler32,
            "file-md5"          : file_md5,
            "segment-adler32"   : segment_adler32,
            "segment-md5"       : segment_md5,
        }
    else:
        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "timestamp"         : hint.timestamp,
            "sequence"          : sequence,
            "key"               : hint.key, 
            "version-number"    : hint.version_number,
            "segment-number"    : hint.segment_number,
            "segment-size"      : len(data)
        }
            
    writer_client.queue_message_for_send(message, data=data)
    reply = yield message_id

    if reply["message-type"] == "archive-key-final-reply":
        # handoff done, tell the backup nodes to destroy the key
        # 2011-05-28 dougfort -- destroy key will destroy everything in the
        # database for the key, not just our segment. So I'm leaving that
        # part out until we switch to postgres.
#        message = {
#            "message-type"      : "destroe-key",
#            "avatar-id"         : hint.avatar_id, 
#            "timestamp"         : hint.timestamp, 
#            "key"               : hint.key, 
#            "version-number"    : hint.version_number,
#            "segment-number"    : hint.segment_number,
#        }
#        for backup_writer_client in backup_writer_clients:
#            message_id = uuid.uuid1().hex
#            message["message-id"] = message_id
#            backup_writer_client.queue_message_for_send(message, data=None)
#            reply = yield message_id
#            assert reply["message-type"] == "destroy-key-reply"
#            if reply["result"] != "success":
#                log.error("destroy-key failed %s" % (reply, ))

        # we give back the hint as our last yield
        yield hint
        return 

    assert reply["message-type"] == "archive-key-start-reply", reply

    sequence += 1

    # send the intermediate segments
    while sequence < segment_count-1:
        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-next",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "key"               : hint.key,
            "version-number"    : hint.version_number,
            "segment-number"    : hint.segment_number,
            "sequence"          : sequence,
        }
        reader_client.queue_message_for_send(message, data=None)
        reply, data = yield message_id
        assert reply["message-type"] == "retrieve-key-next-reply", reply
        assert reply["result"] == "success", reply

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "archive-key-next",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "key"               : hint.key,
            "version-number"    : hint.version_number,
            "segment-number"    : hint.segment_number,
            "sequence"          : sequence,
        }
        
        writer_client.queue_message_for_send(message, data=data)
        reply = yield message_id
        assert reply["message-type"] == "archive-key-next-reply", reply
        assert reply["result"] == "success", reply

        sequence += 1

    # retrieve and archive the last segment
    message_id = uuid.uuid1().hex
    message = {
        "message-type"      : "retrieve-key-final",
        "message-id"        : message_id,
        "avatar-id"         : hint.avatar_id,
        "key"               : hint.key,
        "version-number"    : hint.version_number,
        "segment-number"    : hint.segment_number,
        "sequence"          : sequence,
    }
    reader_client.queue_message_for_send(message, data=None)
    reply, data = yield message_id
    assert reply["message-type"] == "retrieve-key-final-reply", reply
    assert reply["result"] == "success", reply

    message_id = uuid.uuid1().hex
    message = {
        "message-type"      : "archive-key-final",
        "message-id"        : message_id,
        "avatar-id"         : hint.avatar_id,
        "key"               : hint.key,
        "version-number"    : hint.version_number,
        "segment-number"    : hint.segment_number,
        "sequence"          : sequence,
        "total-size"        : total_size,
        "file-adler32"      : file_adler32,
        "file-md5"          : file_md5,
        "segment-adler32"   : segment_adler32,
        "segment-md5"       : segment_md5,
    }
    writer_client.queue_message_for_send(message, data=data)
    reply = yield message_id
    assert reply["message-type"] == "archive-key-final-reply", reply
    assert reply["result"] == "success"

    # handoff done, tell the backup nodes to destroy the key
    # 2011-05-28 dougfort -- destroy key will destroy everything in the
    # database for the key, not just our segment. So I'm leaving that
    # part out until we switch to postgres.
#    for backup_writer_client in backup_writer_clients:
#        message_id = uuid.uuid1().hex
#        message = {
#            "message-type"      : "destroy-key",
#            "message-id"        : message_id,
#            "avatar-id"         : hint.avatar_id, 
#            "timestamp"         : time.time(),
#            "key"               : hint.key, 
#            "version-number"    : hint.version_number,
#            "segment-number"    : hint.segment_number,
#        }
#        backup_writer_client.queue_message_for_send(message, data=None)
#        reply = yield message_id
#        assert reply["message-type"] == "destroy-key-reply"
#        if reply["result"] != "success":
#            log.error("destroy-key failed %s" % (reply, ))

    # we give back the hint as our last yield
    yield hint

