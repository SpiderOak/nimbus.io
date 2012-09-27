# -*- coding: utf-8 -*-
"""
reply_dispatcher.py

a class that dispatches queued replies to messages
"""
import logging
import pickle
import os

from gevent.greenlet import Greenlet

from tools.data_definitions import segment_status_final, \
        segment_status_tombstone, \
        create_priority

from handoff_server.pending_handoffs import PendingHandoffs
from handoff_server.req_socket import ReqSocket, ReqSocketAckTimeOut
from handoff_server.forwarder_coroutine import forwarder_coroutine

class HandoffError(Exception):
    pass

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_node_names = os.environ["NIMBUSIO_NODE_NAME_SEQ"].split()
_data_reader_addresses = \
    os.environ["NIMBUSIO_DATA_READER_ADDRESSES"].split()
_data_writer_addresses = \
    os.environ["NIMBUSIO_DATA_WRITER_ADDRESSES"].split()
_min_handoff_replies = 9

class ReplyDispatcher(Greenlet):
    """
    zmq_context
        zeromq context

    interaction_pool
        pool of database connections

    event_push_client
        client for event notification

    reply_queue
        queue for incoming reply messages

    node_dict
        a dict cross referencing node names and node ids
        in both directions

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 zmq_context,
                 interaction_pool, 
                 event_push_client,
                 reply_queue, 
                 node_dict,
                 halt_event):
        Greenlet.__init__(self)
        self._name = "ReplyDispatcher"

        self._log = logging.getLogger(self._name)

        self._zmq_context = zmq_context
        self._interaction_pool = interaction_pool
        self._event_push_client = event_push_client
        self._reply_queue = reply_queue
        self._node_dict = node_dict
        self._halt_event = halt_event

        self._pending_handoffs = PendingHandoffs()
        self._handoff_reply_count = 0
        self._handoff_replies_already_seen = set()

        self._reader_address_dict = \
            dict(zip(_node_names, _data_reader_addresses))

        self._writer_address_dict = \
            dict(zip(_node_names, _data_writer_addresses))

        self._reader_socket_dict = dict()
        self._writer_socket_dict = dict()
        self._writer_socket_dict[_local_node_name] = \
            ReqSocket(self._zmq_context,
                      self._writer_address_dict[_local_node_name],
                      self._halt_event)
        self._forwarder = None
        self._active_deletes = dict()

        self._dispatch_table = {
            "request-handoffs-reply" : self._handle_request_handoffs_reply,
            "retrieve-key-reply" : self._handle_retrieve_key_reply,
            "archive-key-start-reply" : self._handle_archive_reply,
            "archive-key-next-reply" : self._handle_archive_reply,
            "archive-key-final-reply" : self._handle_archive_reply,
            "destroy-key-reply" : self._handle_destroy_key_reply,
            "purge-handoff-conjoined-reply" : \
                self._handle_purge_handoff_conjoined_reply,
            "purge-handoff-segment-reply"   : \
                self._handle_purge_handoff_segment_reply,
        }

    def __str__(self):
        return self._name

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def _run(self):
        while not self._halt_event.is_set():
            message = self._reply_queue.get()
            if not message.control["message-type"] in self._dispatch_table:
                error_message = "unidentified message-type {0}".format(
                    message.control)
                self._event_push_client.error("handoff-reply-error",
                                              error_message)
                self._log.error(error_message)
                continue

            try:
                self._dispatch_table[message.control["message-type"]](
                    message)
            except Exception, instance:
                error_message = "exception during {0} {1}".format(
                    message.control["message-type"], str(instance))
                self._event_push_client.exception("handoff-reply-error",
                                              error_message)
                self._log.exception(error_message)
                continue

    def _send_message(self, node_names, message):
        for node_name in node_names:
            if not node_name in self._writer_socket_dict:
                self._writer_socket_dict[node_name] = \
                    ReqSocket(self._zmq_context,
                              self._writer_address_dict[node_name],
                              self._halt_event)
            req_socket = self._writer_socket_dict[node_name]
            req_socket.send(message)
        for node_name in node_names:
            req_socket = self._writer_socket_dict[node_name]
            try:
                req_socket.wait_for_ack()
            except ReqSocketAckTimeOut, instance:
                self._log.error("timeout waiting ack {0} {1}".format(
                    str(req_socket), str(instance)))
                del(self._writer_socket_dict[node_name])

    def _handle_request_handoffs_reply(self, message):
        self._log.info(
            "node {0} {1} conjoined-count={2} segment-count={3} {4}".format(
            message.control["node-name"], 
            message.control["result"], 
            message.control["conjoined-count"], 
            message.control["segment-count"], 
            message.control["request-timestamp-repr"]))

        if message.control["result"] != "success":
            error_message = \
                "request-handoffs failed on node {0} {1} {2}".format(
                message.control["node-name"], 
                message.control["result"], 
                message.control["error-message"])
            self._event_push_client.error("handoff-reply-error",
                                          error_message)
            self._log.error(error_message)
            return

        if message.control["conjoined-count"] == 0 and \
            message.control["segment-count"] == 0:
            self._log.info("no handoffs from {0}".format(
                message.control["node-name"]))
            return

        try:
            data_dict = pickle.loads(message.body)
        except Exception, instance:
            error_message = "unable to load handoffs from {0} {1}".format(
                message.control["node-name"], str(instance))
            self._event_push_client.exception("handoff-reply-error",
                                              error_message)
            self._log.exception(error_message)
            return

        source_node_name = message.control["node-name"]

        segment_count  = 0
        already_seen_count = 0
        for segment_row in data_dict["segment"]:
            cache_key = (segment_row["id"], source_node_name, )
            if cache_key in self._handoff_replies_already_seen:
                already_seen_count += 1
                continue
            self._handoff_replies_already_seen.add(cache_key)
            self._pending_handoffs.push(segment_row, source_node_name)
            segment_count += 1
        if already_seen_count > 0:
            self._log.info("ignored {0} handoff segments: already seen".format(
                already_seen_count))
        if segment_count > 0:
            self._log.info("pushed {0} handoff segments".format(segment_count))
        
        self._handoff_reply_count += 1
        self._log.info("{0} handoff replies out of {1}".format(
            self._handoff_reply_count, _min_handoff_replies))

        if self._handoff_reply_count == _min_handoff_replies:
            self._log.info("found {0} segment handoffs".format(
                len(self._pending_handoffs)))
            self._start_handoff()

    def _handle_retrieve_key_reply(self, message, data):
        # 2012-02-05 dougfort -- handle a race condition where we pick up a segment
        # to be handed off after we have purged it, because the message was 
        # in transit
        if message["result"] == "no-sequence-rows":
            self._log.debug("no-sequence-rows, assuming already purged {0}".format(
                message))
            self._forwarder = None
            return

        if message["result"] != "success":
            error_message = "%s failed (%s) %s %s" % (
                message["message-type"], 
                message["result"], 
                message["error-message"], 
                message,
            )
            self._log.error(error_message)
            raise HandoffError(error_message)

        self._forwarder.send((message, data, ))

    def _handle_archive_reply(self, message, _data):
        #TODO: we need to squawk about this somehow
        if message["result"] != "success":
            error_message = "%s failed (%s) %s %s" % (
                message["message-type"], 
                message["result"], 
                message["error-message"], 
                message,
            )
            self._log.error(error_message)
            raise HandoffError(error_message)

        result = self._forwarder.send(message)

        if result is not None:
            self._forwarder = None

            segment_row, source_node_names = result

            description = "handoff complete %s %s %s %s" % (
                segment_row["collection_id"],
                segment_row["key"],
                segment_row["timestamp"],
                segment_row["segment_num"],
            )
            self._log.info(description)

            self._event_push_client.info(
                "handoff-complete",
                description,
                backup_sources=source_node_names,
                collection_id=segment_row["collection_id"],
                key=segment_row["key"],
                timestamp_repr=repr(segment_row["timestamp"])
            )
            
            # purge the handoff source(s)
            message = {
                "message-type"      : "purge-handoff-segment",
                "priority"          : create_priority(),
                "unified-id"        : segment_row["unified_id"],
                "conjoined-part"    : segment_row["conjoined_part"],
                "handoff-node-id"   : segment_row["handoff_node_id"]
            }
            self._send_message(source_node_names, message)

        self._forwarder = None

        self._dispatch_table = {
            "request-handoffs-reply" : self._handle_request_handoffs_reply,
            "retrieve-key-reply" : self._handle_retrieve_key_reply,
            "archive-key-start-reply" : self._handle_archive_reply,
            "archive-key-next-reply" : self._handle_archive_reply,
            "archive-key-final-reply" : self._handle_archive_reply,
            "destroy-key-reply" : self._handle_destroy_key_reply,
            "purge-handoff-conjoined-reply" : \
                self._handle_purge_handoff_conjoined_reply,
            "purge-handoff-segment-reply"   : \
                self._handle_purge_handoff_segment_reply,
        }

    def _handle_destroy_key_reply(self, message, _data):
        #TODO: we need to squawk about this somehow
        if message["result"] != "success":
            error_message = "%s failed (%s) %s %s" % (
                message["message-type"], 
                message["result"], 
                message["error-message"], 
                message,
            )
            self._log.error(error_message)
            raise HandoffError(error_message)

        try:
            source_node_names = self._active_deletes.pop(message["unified-id"])
        except KeyError:
            self._log.error("unknown reply %s" % (message["unified-id"], ))
            return

        # purge the handoff source(s)
        message = {
            "message-type"      : "purge-handoff-segment",
            "priority"          : create_priority(),
            "unified-id"        : message["unified-id"],
            "conjoined-part"    : 0,
            "handoff-node-id"   : self._node_dict[_local_node_name],
        }
        self._send_message(source_node_names, message)

    def _handle_purge_handoff_conjoined_reply(self, message, _data):
        #TODO: we need to squawk about this somehow
        if message["result"] == "success":
            self._log.debug("purge-key successful")
        else:
            self._log.error("%s failed (%s) %s %s" % (
                message["message-type"], 
                message["result"], 
                message["error-message"], 
                message,
            ))
            # we don't give up here, because the handoff has succeeded 
            # at this point we're just cleaning up

    def _handle_purge_handoff_segment_reply(self, message, _data):
        #TODO: we need to squawk about this somehow
        if message["result"] == "success":
            self._log.debug("purge-key successful")
        else:
            self._log.error("%s failed (%s) %s %s" % (
                message["message-type"], 
                message["result"], 
                message["error-message"], 
                message,
            ))
            # we don't give up here, because the handoff has succeeded 
            # at this point we're just cleaning up

    def _start_handoff(self):
        try:
            segment_row, source_node_names = \
                    self._pending_handoffs.pop()
        except IndexError:
            self._log.debug("_start_handoffs: no handoffs found")
            return

        if segment_row["status"] == segment_status_final:
            self._start_forwarder_coroutine(segment_row, source_node_names)
            return

        if segment_row["status"] == segment_status_tombstone:
            self._send_delete_message(segment_row, source_node_names)
            return

        error_message = \
            "unknown segment status '{0}' ({1}) {2} part={3}".format(
            segment_row["status"],
            segment_row["collection_id"],
            segment_row["key"],
            segment_row["conjoined_part"])
        self._log.error(error_message)
        self._event_push_client.error("handoff-start-error",
                                      error_message)


    def _start_forwarder_coroutine(self, segment_row, source_node_names):
        # we pick the first source name, because it's easy, and because,
        # since that source responded to us first, it might have better 
        # response
        # TODO: switch over to the second source on error
        source_node_name = source_node_names[0]
        if not source_node_name in self._reader_socket_dict:
            self._reader_socket_dict[source_node_name] = \
                ReqSocket(self._zmq_context,
                          source_node_name,
                          self._halt_event)
        reader_socket = self._reader_socket_dict[source_node_name]
        writer_socket = self._writer_socket_dict[_local_node_name]

        description = "start handoff from %s to %s (%s) %r part=%s" % (
            source_node_name,
            self._local_node_name,
            segment_row["collection_id"],
            segment_row["key"],
            segment_row["conjoined_part"]
        )
        self._log.info(description)

        self._event_push_client.info(
            "handoff-start",
            description,
            backup_source=source_node_name,
            collection_id=segment_row["collection_id"],
            key=segment_row["key"],
            timestamp_repr=repr(segment_row["timestamp"])
        )
        
        assert self._forwarder is None
        self._forwarder = forwarder_coroutine(self._node_dict,
                                              segment_row, 
                                              source_node_names, 
                                              writer_socket, 
                                              reader_socket)
        self._forwarder.next()

    def _send_delete_message(self, segment_row, source_node_names):
        source_node_name = source_node_names[0]
        message = {
            "message-type"          : "destroy-key",
            "priority"              : create_priority(),
            "collection-id"         : segment_row["collection_id"],
            "key"                   : segment_row["key"],
            "unified-id-to-delete"  : segment_row["file_tombstone_unified_id"],
            "unified-id"            : segment_row["unified_id"],
            "timestamp-repr"        : repr(segment_row["timestamp"]),
            "segment-num"           : segment_row["segment_num"],
            "source-node-name"      : \
                self._node_dict[segment_row["source_node_id"]],
            "handoff-node-name"     : None,
        }
        self._send_message(source_node_names, message)
        self._active_deletes[segment_row["unified_id"]] = source_node_names

