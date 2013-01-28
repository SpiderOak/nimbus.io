# -*- coding: utf-8 -*-
"""
forwarder_coroutine.py

a coroutine that handles message traffic for retrieving and
re-archiving a segment that was handed off to us
"""
import logging
import uuid

from tools.data_definitions import create_priority

from handoff_client.req_socket import ReqSocketAckTimeOut

def forwarder_coroutine(node_dict, segment_row, writer_socket, reader_socket):
    """
    manage the message traffic for retrieving and re-archiving 
    a segment that was handed off to us
    """
    log = logging.getLogger("forwarder_coroutine")
    user_request_id = str(uuid.uuid4())
    archive_priority = create_priority()
    retrieve_id = uuid.uuid1().hex
    retrieve_sequence = 0

    # start retrieving from our reader
    message = {
        "message-type"              : "retrieve-key-start",
        "user-request-id"           : user_request_id,
        "retrieve-id"               : retrieve_id,
        "retrieve-sequence"         : retrieve_sequence,
        "collection-id"             : segment_row["collection_id"],
        "key"                       : segment_row["key"],
        "segment-unified-id"        : segment_row["unified_id"],
        "segment-conjoined-part"    : segment_row["conjoined_part"],
        "segment-num"               : segment_row["segment_num"],
        "handoff-node-id"           : segment_row["handoff_node_id"],
        "block-offset"              : 0,
        "block-count"               : None,
    }

    log.debug("request {0}: " \
              "sending retrieve-key-start {1} {2}".format(
              user_request_id,
              segment_row["unified_id"], 
              segment_row["segment_num"]))
    
    reader_socket.send(message)
    try:
        reader_socket.wait_for_ack()
    except ReqSocketAckTimeOut as instance:
        log.error("request {0}: " \
                  "timeout waiting ack {1} {2}".format(user_request_id,
                                                       str(reader_socket), 
                                                       str(instance)))
        raise

    reply, data = yield

    assert reply["message-type"] == "retrieve-key-reply", reply
    assert reply["result"] == "success", reply
    completed = reply["completed"]

    sequence = 1

    if completed:
        message = {
            "message-type"      : "archive-key-entire",
            "user-request-id"   : user_request_id,
            "priority"          : archive_priority,
            "collection-id"     : segment_row["collection_id"],
            "key"               : segment_row["key"], 
            "unified-id"        : segment_row["unified_id"],
            "conjoined-part"    : segment_row["conjoined_part"],
            "timestamp-repr"    : repr(segment_row["timestamp"]),
            "segment-num"       : segment_row["segment_num"],
            "segment-size"      : reply["segment-size"],
            "zfec-padding-size" : reply["zfec-padding-size"],
            "segment-adler32"   : reply["segment-adler32"],
            "segment-md5-digest": reply["segment-md5-digest"],
            "file-size"         : segment_row["file_size"],
            "file-adler32"      : segment_row["file_adler32"],
            "file-hash"         : segment_row["file_hash"],
            "source-node-name"  : node_dict[segment_row["source_node_id"]],
            "handoff-node-name" : None,
        }
    else:
        message = {
            "message-type"      : "archive-key-start",
            "user-request-id"   : user_request_id,
            "priority"          : archive_priority,
            "collection-id"     : segment_row["collection_id"],
            "key"               : segment_row["key"], 
            "unified-id"        : segment_row["unified_id"],
            "conjoined-part"    : segment_row["conjoined_part"],
            "timestamp-repr"    : repr(segment_row["timestamp"]),
            "segment-num"       : segment_row["segment_num"],
            "segment-size"      : reply["segment-size"],
            "zfec-padding-size" : reply["zfec-padding-size"],
            "segment-adler32"   : reply["segment-adler32"],
            "segment-md5-digest": reply["segment-md5-digest"],
            "sequence-num"      : sequence,
            "source-node-name"  : node_dict[segment_row["source_node_id"]],
            "handoff-node-name" : None,
        }
            
    writer_socket.send(message, data=data)
    try:
        writer_socket.wait_for_ack()
    except ReqSocketAckTimeOut as instance:
        log.error("request {0}: " \
                  "timeout waiting ack {1} {2}".format(user_request_id,
                                                       str(writer_socket), 
                                                       str(instance)))
        raise
    reply, _ = yield

    if completed:
        yield "done"
        return 

    assert reply["message-type"] == "archive-key-start-reply", reply
    assert reply["result"] == "success", reply

    # send the intermediate segments
    while not completed:
        retrieve_sequence += 1
        sequence += 1

        message = {
            "message-type"              : "retrieve-key-next",
             "user-request-id"          : user_request_id,
            "retrieve-id"               : retrieve_id,
            "retrieve-sequence"         : retrieve_sequence,
            "collection-id"             : segment_row["collection_id"],
            "key"                       : segment_row["key"],
            "segment-unified-id"        : segment_row["unified_id"],
            "segment-conjoined-part"    : segment_row["conjoined_part"],
            "segment-num"               : segment_row["segment_num"],
            "handoff-node-id"           : segment_row["handoff_node_id"],
            "block-offset"              : 0,
            "block-count"               : None,
        }
        reader_socket.send(message)
        try:
            reader_socket.wait_for_ack()
        except ReqSocketAckTimeOut as instance:
            log.error("request {0}: " \
                      "timeout waiting ack {1} {2}".format(user_request_id,
                                                           str(reader_socket), 
                                                           str(instance)))
            raise

        reply, data = yield
        assert reply["message-type"] == "retrieve-key-reply", reply
        assert reply["result"] == "success", reply
        completed = reply["completed"]

        if completed:
            message = {
                "message-type"      : "archive-key-final",
                "user-request-id"   : user_request_id,
                "priority"          : archive_priority,
                "collection-id"     : segment_row["collection_id"],
                "key"               : segment_row["key"],
                "unified-id"        : segment_row["unified_id"],
                "conjoined-part"    : segment_row["conjoined_part"],
                "timestamp-repr"    : repr(segment_row["timestamp"]),
                "segment-num"       : segment_row["segment_num"],
                "segment-size"      : reply["segment-size"],
                "zfec-padding-size" : reply["zfec-padding-size"],
                "segment-adler32"   : reply["segment-adler32"],
                "segment-md5-digest": reply["segment-md5-digest"],
                "sequence-num"      : sequence,
                "file-size"         : segment_row["file_size"],
                "file-adler32"      : segment_row["file_adler32"],
                "file-hash"         : segment_row["file_hash"],
                "source-node-name"  : node_dict[segment_row["source_node_id"]],
                "handoff-node-name" : None,
            }
        else:
            message = {
                "message-type"      : "archive-key-next",
                "user-request-id"   : user_request_id,
                "priority"          : archive_priority,
                "collection-id"     : segment_row["collection_id"],
                "key"               : segment_row["key"],
                "unified-id"        : segment_row["unified_id"],
                "conjoined-part"    : segment_row["conjoined_part"],
                "timestamp-repr"    : repr(segment_row["timestamp"]),
                "segment-num"       : segment_row["segment_num"],
                "segment-size"      : reply["segment-size"],
                "zfec-padding-size" : reply["zfec-padding-size"],
                "segment-adler32"   : reply["segment-adler32"],
                "segment-md5-digest": reply["segment-md5-digest"],
                "sequence-num"      : sequence,
                "source-node-name"  : node_dict[segment_row["source_node_id"]],
                "handoff-node-name" : None,
            }
        
        writer_socket.send(message, data=data)
        try:
            writer_socket.wait_for_ack()
        except ReqSocketAckTimeOut as instance:
            log.error("request {0}: " \
                      "timeout waiting ack {1} {2}".format(user_request_id,
                                                           str(writer_socket), 
                                                           str(instance)))
            raise
        reply, _ = yield
        assert reply["result"] == "success", reply

    yield "done"

