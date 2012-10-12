# -*- coding: utf-8 -*-
"""
handoff_manager.py

a class that manages segment handoffs
"""
import logging
import os

from gevent.greenlet import Greenlet

from tools.data_definitions import segment_status_final, \
        segment_status_tombstone, \
        create_priority
from tools.LRUCache import LRUCache

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
_already_processed_cache_size = 10000

class HandoffManager(Greenlet):
    """
    zmq_context
        zeromq context

    interaction_pool
        pool of database connections

    event_push_client
        client for event notification

    pending_handoffs
        an ordered queue of segments rows and supporting information

    reply_queue
        queue for incoming reply messages

    node_dict
        a dict cross referencing node names and node ids
        in both directions

    client_tag
        A unique identifier for our client, to be included in every message

    client_address
        the address our socket binds to. Sent to the remote server in every
        message

    halt_event:
        Event object, set when it's time to halt
    """
    def __init__(self, 
                 zmq_context,
                 interaction_pool, 
                 event_push_client,
                 pending_handoffs,
                 reply_queue, 
                 node_dict,
                 client_tag,
                 client_address,
                 halt_event):
        Greenlet.__init__(self)
        self._name = "HandoffManager"

        self._log = logging.getLogger(self._name)

        self._zmq_context = zmq_context
        self._interaction_pool = interaction_pool
        self._event_push_client = event_push_client
        self._pending_handoffs = pending_handoffs
        self._reply_queue = reply_queue
        self._node_dict = node_dict
        self._client_tag = client_tag
        self._client_address = client_address
        self._halt_event = halt_event

        self._reader_address_dict = \
            dict(zip(_node_names, _data_reader_addresses))

        self._writer_address_dict = \
            dict(zip(_node_names, _data_writer_addresses))

        self._reader_socket_dict = dict()
        self._writer_socket_dict = dict()
        self._writer_socket_dict[_local_node_name] = \
            ReqSocket(self._zmq_context,
                      self._writer_address_dict[_local_node_name],
                      self._client_tag,
                      self._client_address,
                      self._halt_event)
        self._forwarder = None
        self._active_deletes = dict()
        self._handoff_complete = True

        self._dispatch_table = {
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

        self._already_processed_cache = LRUCache(_already_processed_cache_size)

    def __str__(self):
        return self._name

    def join(self, timeout=3.0):
        """
        Clean up and wait for the greenlet to shut down
        """
        self._log.debug("joining")
        Greenlet.join(self, timeout)
        for req_socket in self._reader_socket_dict.values():
            req_socket.close()
        for req_socket in self._writer_socket_dict.values():
            req_socket.close()
        self._log.debug("join complete")

    def _run(self):
        while not self._halt_event.is_set():
            try:
                segment_row, source_node_name = self._pending_handoffs.pop()
            except IndexError:
                self._halt_event.wait(1.0)
                continue

            if self._already_processed(segment_row):
                # purge the handoff source
                purge_request = {
                    "message-type"      : "purge-handoff-segment",
                    "priority"          : create_priority(),
                    "unified-id"        : segment_row["unified_id"],
                    "conjoined-part"    : segment_row["conjoined_part"],
                    "handoff-node-id"   : segment_row["handoff_node_id"]
                }
                self._send_message(source_node_name, purge_request)
                continue

            self._start_handoff(segment_row, source_node_name)
            self._handoff_complete = False

            while not self._halt_event.is_set() and not self._handoff_complete:
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

    def _already_processed(self, segment_row):
        """
        I don't think it's wise that we purge one of the handoff segments 
        before we've actually completed receiving the handoff. 
        I suggest working around it like this:

        Heave the existing code that marks a given handoff in the queue as a 
        duplicate

        At the beginning of processing every handoff, 
        check with our local database to see if we already have a final 
        version of this segment. 
        
        If we do, THEN send the purge message and move on.

        (Optional) make #2 fast by adding a LRUCache for, say, the 10,000 
        previously successfully received handoffs. 
        This will avoid the DB lookup in the common case.
        """
        cache_key = (segment_row["unified_id"], segment_row["conjoined_part"], )
        if cache_key in self._already_processed_cache:
            del self._already_processed_cache[cache_key]
            return True

        query = """select status from nimbusio_node.segment
                   where collection_id = %s and key = %s
                   and unified_id = %s and conjoined_part = %s"""
        args = [segment_row["collection_id"], 
                segment_row["key"],
                segment_row["unified_id"],
                segment_row["conjoined_part"]]
        async_result = self._interaction_pool.run(interaction=query, 
                                                  interaction_args=args,
                                                  pool=_local_node_name) 
        result_list = async_result.get()

        if len(result_list) == 0:
            return False

        assert len(result_list) == 1, result_list
        if result_list[0]["status"] not in [segment_status_final, 
                                            segment_status_tombstone]:
            return False

        self._already_processed_cache[cache_key] = True
        return True

    def _send_message(self, node_name, message):
        if not node_name in self._writer_socket_dict:
            self._writer_socket_dict[node_name] = \
                ReqSocket(self._zmq_context,
                          self._writer_address_dict[node_name],
                          self._client_tag,
                          self._client_address,
                          self._halt_event)
        req_socket = self._writer_socket_dict[node_name]

        req_socket.send(message)
        try:
            req_socket.wait_for_ack()
        except ReqSocketAckTimeOut, instance:
            self._log.error("timeout waiting ack {0} {1}".format(
                str(req_socket), str(instance)))
            del(self._writer_socket_dict[node_name])

    def _handle_retrieve_key_reply(self, message):
        # 2012-02-05 dougfort -- handle a race condition where we pick up a segment
        # to be handed off after we have purged it, because the message was 
        # in transit
        if message.control["result"] == "no-sequence-rows":
            self._log.debug("assuming already purged {0}".format(
                message.control))
            self._forwarder = None
            return

        if message.control["result"] != "success":
            error_message = "%s failed (%s) %s %s" % (
                message.control["message-type"], 
                message.control["result"], 
                message.control["error-message"], 
                message,
            )
            self._log.error(error_message)
            raise HandoffError(error_message)

        self._forwarder.send((message.control, message.body, ))

    def _handle_archive_reply(self, message):
        #TODO: we need to squawk about this somehow
        if message.control["result"] != "success":
            error_message = "%s failed (%s) %s %s" % (
                message.control["message-type"], 
                message.control["result"], 
                message.control["error-message"], 
                message.control,
            )
            self._log.error(error_message)
            raise HandoffError(error_message)

        result = self._forwarder.send(message)

        if result is not None:
            self._forwarder = None

            segment_row, source_node_name = result

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
                backup_source=source_node_name,
                collection_id=segment_row["collection_id"],
                key=segment_row["key"],
                timestamp_repr=repr(segment_row["timestamp"])
            )

            self._handoff_complete = True
            
            # purge the handoff source
            message = {
                "message-type"      : "purge-handoff-segment",
                "priority"          : create_priority(),
                "unified-id"        : segment_row["unified_id"],
                "conjoined-part"    : segment_row["conjoined_part"],
                "handoff-node-id"   : segment_row["handoff_node_id"]
            }
            self._send_message(source_node_name, message)

    def _handle_destroy_key_reply(self, message):
        #TODO: we need to squawk about this somehow
        if message.control["result"] != "success":
            error_message = "%s failed (%s) %s %s" % (
                message.control["message-type"], 
                message.control["result"], 
                message.control["error-message"], 
                message,
            )
            self._log.error(error_message)
            raise HandoffError(error_message)

        try:
            source_node_name = \
                self._active_deletes.pop(message.control["unified-id"])
        except KeyError:
            self._log.error("unknown reply %s" % (message.control["unified-id"], ))
            return

        # purge the handoff source(s)
        message = {
            "message-type"      : "purge-handoff-segment",
            "priority"          : create_priority(),
            "unified-id"        : message.control["unified-id"],
            "conjoined-part"    : 0,
            "handoff-node-id"   : self._node_dict[_local_node_name],
        }
        self._send_message(source_node_name, message)

    def _handle_purge_handoff_conjoined_reply(self, message):
        #TODO: we need to squawk about this somehow
        if message.control["result"] == "success":
            self._log.debug("purge-key successful")
        else:
            self._log.error("%s failed (%s) %s %s" % (
                message.control["message-type"], 
                message.control["result"], 
                message.control["error-message"], 
                message.control,
            ))
            # we don't give up here, because the handoff has succeeded 
            # at this point we're just cleaning up

    def _handle_purge_handoff_segment_reply(self, message):
        #TODO: we need to squawk about this somehow
        if message.control["result"] == "success":
            self._log.debug("purge-key successful")
        else:
            self._log.error("%s failed (%s) %s %s" % (
                message.control["message-type"], 
                message.control["result"], 
                message.control["error-message"], 
                message.control,
            ))
            # we don't give up here, because the handoff has succeeded 
            # at this point we're just cleaning up

    def _start_handoff(self, segment_row, source_node_name):
        self._log.debug(
            "_start_handoff segment_row = {0} source_node_name = {1}".format(
                segment_row, source_node_name))

        if segment_row["status"] == segment_status_final:
            self._start_forwarder_coroutine(segment_row, source_node_name)
            return

        if segment_row["status"] == segment_status_tombstone:
            self._send_delete_message(segment_row, source_node_name)
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


    def _start_forwarder_coroutine(self, segment_row, source_node_name):
        if not source_node_name in self._reader_socket_dict:
            self._reader_socket_dict[source_node_name] = \
                ReqSocket(self._zmq_context,
                          self._reader_address_dict[source_node_name],
                          self._client_tag,
                          self._client_address,
                          self._halt_event)
        reader_socket = self._reader_socket_dict[source_node_name]
        writer_socket = self._writer_socket_dict[_local_node_name]

        description = "start handoff from %s to %s (%s) %r part=%s" % (
            source_node_name,
            _local_node_name,
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
                                              source_node_name, 
                                              writer_socket, 
                                              reader_socket)
        self._forwarder.next()

    def _send_delete_message(self, segment_row, source_node_name):
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
        writer_socket = self._writer_socket_dict[_local_node_name]
        writer_socket.send(message, data=None)
        try:
            writer_socket.wait_for_ack()
        except ReqSocketAckTimeOut, instance:
            self._log.error("timeout waiting ack {0} {1}".format(
                str(writer_socket), str(instance)))
            raise
        self._active_deletes[segment_row["unified_id"]] = source_node_name

