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
    hint, 
    database_connection, 
    writer_client, 
    reader_client, 
    backup_writer_clients
):
    """
    manage the message traffic for retrieving and re-archiving 
    a segment that was handed off to us
    """
    log = logging.getLogger("forwarder_coroutine")

    # get file info from the local database
    file_info = most_recent_timestamp_for_key(
        self._database_connection, hint.avatar_id, hint.key
    )
    if file_info is None:
        error_string = "No data for %s %s" % (hint.avatar_id, hint.key)
        log.error(error_string)
        raise ValueError(error_string)

    # start retrieving from our reader
    message_id = uuid.uuid1().hex
    message = {
        "message-type"      : "retrieve-key-start",
        "message-id"        : message_id,
        "avatar-id"         : hint.avatar_id,
        "key"               : hint.key,
        "timestamp-repr"    : repr(hint.timestamp),
        "segment-num"       : hint.segment_num,
    }

    log.debug("sending retrieve-key-start %s %s %s %s" % (
        hint.avatar_id, hint.key, hint.timestamp, hint.segment_num
    ))
    
    reader_client.queue_message_for_send(message, data=None)
    reply, data = yield message_id

    assert reply["message-type"] == "retrieve-key-reply", reply
    assert reply["result"] == "success", reply
    completed = reply["completed"]

    sequence = 1

    message_id = uuid.uuid1().hex
    if completed:
        message = {
            "message-type"      : "archive-key-entire",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "key"               : hint.key, 
            "timestamp-repr"    : repr(hint.timestamp),
            "segment-num"       : hint.segment_num,
            "file-size"         : file_info.file_size,
            "file-adler32"      : file_info.file_adler32,
            "file-hash"         : b64encode(file_info.file_hash),
            "file-user-id"      : file_info.file_user_id,
            "file-group-id"     : file_info.file_group_id,
            "file-permissions"  : file_info.file_permissions,
            "handoff-node-id"   : None,
        }
    else:
        message = {
            "message-type"      : "archive-key-start",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "key"               : hint.key, 
            "timestamp-repr"    : repr(hint.timestamp),
            "segment-num"       : hint.segment_num,
            "sequence-num"      : sequence,
        }
            
    writer_client.queue_message_for_send(message, data=data)
    reply = yield message_id

    if completed:
        assert reply["message-type"] == "archive-key-start-reply", reply
        assert reply["result"] == "success", reply

        message = {
            "message-type"      : "purge-key",
            "avatar-id"         : hint.avatar_id, 
            "key"               : hint.key, 
            "timestamp-repr"    : hrepr(int.timestamp), 
            "segment-num"       : hint.segment_num,
        }
        for backup_writer_client in backup_writer_clients:
            message_id = uuid.uuid1().hex
            message["message-id"] = message_id
            backup_writer_client.queue_message_for_send(message, data=None)
            reply = yield message_id
            assert reply["message-type"] == "destroy-key-reply"
            if reply["result"] != "success":
                log.error("purge-key failed %s" % (reply, ))

        # we give back the hint as our last yield
        yield hint
        return 

    assert reply["message-type"] == "archive-key-start-reply", reply
    assert reply["result"] == "success", reply

    # send the intermediate segments
    while not completed:
        sequence += 1

        message_id = uuid.uuid1().hex
        message = {
            "message-type"      : "retrieve-key-next",
            "message-id"        : message_id,
            "avatar-id"         : hint.avatar_id,
            "key"               : hint.key,
            "timestamp-repr"    : repr(hint.timestamp),
            "segment-num"       : hint.segment_num,
            "sequence-num"      : sequence,
        }
        reader_client.queue_message_for_send(message, data=None)
        reply, data = yield message_id
        assert reply["message-type"] == "retrieve-key-next-reply", reply
        assert reply["result"] == "success", reply
        completed = message["completed"]

        message_id = uuid.uuid1().hex
        if completed:
            message = {
                "message-type"      : "archive-key-final",
                "message-id"        : message_id,
                "avatar-id"         : hint.avatar_id,
                "key"               : hint.key,
                "segment-number"    : hint.segment_number,
                "sequence-num"      : sequence,
                "file-size"         : file_info.file_size,
                "file-adler32"      : file_info.file_adler32,
                "file-hash"         : b64encode(file_info.file_hash),
                "file-user-id"      : file_info.file_user_id,
                "file-group-id"     : file_info.file_group_id,
                "file-permissions"  : file_info.file_permissions,
                "handoff-node-id"   : None,
            }
        else:
            message = {
                "message-type"      : "archive-key-next",
                "message-id"        : message_id,
                "avatar-id"         : hint.avatar_id,
                "key"               : hint.key,
                "segment-num"       : hint.segment_num,
                "sequence-num"      : sequence,
            }
        
        writer_client.queue_message_for_send(message, data=data)
        reply = yield message_id
        assert reply["message-type"] == "archive-key-next-reply", reply
        assert reply["result"] == "success", reply

    # handoff done, tell the backup nodes to destroy the key
    message = {
        "message-type"      : "purge-key",
        "avatar-id"         : hint.avatar_id, 
        "key"               : hint.key, 
        "timestamp-repr"    : repr(hint.timestamp), 
        "segment-num"       : hint.segment_num,
    }
    for backup_writer_client in backup_writer_clients:
        message_id = uuid.uuid1().hex
        message["message-id"] = message_id
        backup_writer_client.queue_message_for_send(message, data=None)
        reply = yield message_id
        assert reply["message-type"] == "destroy-key-reply"
        if reply["result"] != "success":
            log.error("purge-key failed %s" % (reply, ))

    # we give back the hint as our last yield
    yield hint

