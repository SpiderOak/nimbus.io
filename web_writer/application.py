"""
application.py

The nimbus.io wsgi application

for a write:
at startup time, web writer creates resilient_client to each node
application:
archive:
  at request time, creates DataWriter for each node, regardless of connection
  each DataWriter will have either a ResilientClient for a connected node
   OR will have a HandoffClient which wraps two ResilientClients on behalf of
      a disconnected node
      ResilientClient = tools/greenlet_resilient_client.py
      HandoffClient = web_writer/data_writer_handoff_client.py



"""
import logging
import os
import random
import zlib
import hashlib
import json
import urllib
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import incoming_slice_size, \
        block_generator, \
        create_priority, \
        create_timestamp, \
        nimbus_meta_prefix

from tools.zfec_segmenter import ZfecSegmenter

from web_writer.exceptions import ArchiveFailedError, \
        DestroyFailedError, \
        ConjoinedFailedError

from web_writer.data_writer_handoff_client import DataWriterHandoffClient
from web_writer.data_writer import DataWriter
from web_writer.data_slicer import DataSlicer
from web_writer.archiver import Archiver
from web_writer.destroyer import Destroyer
from web_writer.conjoined_manager import start_conjoined_archive, \
        abort_conjoined_archive, \
        finish_conjoined_archive
from web_writer.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_archive_key, \
        action_delete_key, \
        action_start_conjoined, \
        action_finish_conjoined, \
        action_abort_conjoined

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_reply_timeout = float(
    os.environ.get("NIMBUS_IO_REPLY_TIMEOUT",  str(5 * 60.0))
)
_min_connected_clients = 8
_min_segments = 8
_max_segments = 10
_handoff_count = 2

_s3_meta_prefix = "x-amz-meta-"
_sizeof_s3_meta_prefix = len(_s3_meta_prefix)
_archive_retry_interval = 120
_content_type_json = "application/json"

def _fix_timestamp(timestamp):
    return (None if timestamp is None else repr(timestamp))

def _build_meta_dict(req_get):
    """
    create a dict of meta values, conveting the aws prefix to ours
    """
    meta_dict = dict()
    for key in req_get:
        if key.startswith(_s3_meta_prefix):
            converted_key = "".join(
                    [nimbus_meta_prefix, key[_sizeof_s3_meta_prefix:]]
                )
            meta_dict[converted_key] = req_get[key]
        elif key.startswith(nimbus_meta_prefix):
            meta_dict[key] = req_get[key]

    return meta_dict

def _connected_clients(clients):
    return [client for client in clients if client.connected]

def _create_data_writers(event_push_client, clients):
    data_writers_dict = dict()

    connected_clients_by_node = list()
    disconnected_clients_by_node = list()

    for node_name, client in zip(_node_names, clients):
        if client.connected:
            connected_clients_by_node.append((node_name, client))
        else:
            disconnected_clients_by_node.append((node_name, client))

    if len(connected_clients_by_node) < _min_connected_clients:
        raise exc.HTTPServiceUnavailable("Too few connected writers %s" % (
            len(connected_clients_by_node),
        ))

    connected_clients = list()
    for node_name, client in connected_clients_by_node:
        connected_clients.append(client)
        assert node_name not in data_writers_dict, connected_clients_by_node
        data_writers_dict[node_name] = DataWriter(node_name, client)
    
    for node_name, client in disconnected_clients_by_node:
        backup_clients = random.sample(connected_clients, _handoff_count)
        assert backup_clients[0] != backup_clients[1]
        data_writer_handoff_client = DataWriterHandoffClient(
            client.server_node_name,
            backup_clients
        )
        assert node_name not in data_writers_dict, data_writers_dict
        data_writers_dict[node_name] = DataWriter(
            node_name, data_writer_handoff_client
        )

    # 2011-05-27 dougfort -- the data-writers list must be in 
    # the same order as _node_names, because that's the order that
    # segment numbers get defined in
    return [data_writers_dict[node_name] for node_name in _node_names]

def _send_archive_cancel(unified_id, conjoined_part, clients):
    # message sent to data writers telling them to cancel the archive
    for i, client in enumerate(clients):
        if not client.connected:
            continue
        cancel_message = {
            "message-type"      : "archive-key-cancel",
            "priority"          : create_priority(),
            "unified-id"        : unified_id,
            "conjoined-part"    : conjoined_part,
            "segment-num"       : i+1,
        }
        client.queue_message_for_broadcast(cancel_message)

class Application(object):
    def __init__(
        self, 
        cluster_row,
        unified_id_factory,
        id_translator,
        data_writer_clients, 
        authenticator, 
        accounting_client,
        event_push_client,
        stats
    ):
        self._log = logging.getLogger("Application")
        self._cluster_row = cluster_row
        self._unified_id_factory = unified_id_factory
        self._id_translator = id_translator
        self._data_writer_clients = data_writer_clients
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client
        self._stats = stats


        self._dispatch_table = {
            action_respond_to_ping      : self._respond_to_ping,
            action_archive_key          : self._archive_key,
            action_delete_key           : self._delete_key,
            action_start_conjoined      : self._start_conjoined,
            action_finish_conjoined     : self._finish_conjoined,
            action_abort_conjoined      : self._abort_conjoined,
        }

    @wsgify
    def __call__(self, req):

        result = parse_url(req.method, req.url)
        if result is None:
            self._log.error("Unparseable URL: %r" % (req.url, ))
            raise exc.HTTPNotFound(req.url)

        action_tag, match_object = result
        try:
            return self._dispatch_table[action_tag](req, match_object)
        except exc.HTTPException, instance:
            self._log.error("%s %s %s %r" % (
                instance.__class__.__name__, 
                instance, 
                action_tag,
                req.url
            ))
            raise
        except Exception, instance:
            self._log.exception("%s" % (req.url, ))
            self._event_push_client.exception(
                "unhandled_exception",
                str(instance),
                exctype=instance.__class__.__name__
            )
            raise

    def _respond_to_ping(self, _req, _match_object):
        self._log.debug("_respond_to_ping")
        response = Response(status=200, content_type="text/plain")
        response.body_file.write("ok")
        return response

    def _archive_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = \
                self._authenticator.authenticate(collection_name,
                                                 req)
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        if collection_entry is None:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.error('unable to prepare key %r %s' % (
                key, instance
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        if req.content_length <= 0:
            raise exc.HTTPForbidden(
                "cannot archive: content_length = %s" % (req.content_length, )
            ) 

        start_time = time.time()
        self._stats["archives"] += 1
        description = \
                "archive: collection=(%s)%r customer=%r key=%r, size=%s" % (
            collection_entry.collection_id,
            collection_entry.collection_name,
            collection_entry.username,
            key, 
            req.content_length
        )
        self._log.info(description)

        meta_dict = _build_meta_dict(req.GET)

        unified_id = None
        conjoined_part = 0

        if "conjoined_identifier" in req.GET:
            unified_id = self._id_translator.internal_id(
               req.GET["conjoined_identifier"]
            )
        else:
            unified_id = self._unified_id_factory.next()

        if "conjoined_part" in req.GET:
            value = req.GET["conjoined_part"]
            value = urllib.unquote_plus(value)
            value = value.decode("utf-8")
            if len(value) > 0:
                conjoined_part = int(value)


        data_writers = _create_data_writers(
            self._event_push_client,
            # _data_writer_clients are the 0mq clients for each of the nodes in
            # the cluster. They may or may not be connected.
            self._data_writer_clients
        ) 
        timestamp = create_timestamp()
        archiver = Archiver(
            data_writers,
            collection_entry.collection_id,
            key,
            unified_id,
            timestamp,
            meta_dict,
            conjoined_part
        )
        segmenter = ZfecSegmenter(_min_segments, len(data_writers))
        file_adler32 = zlib.adler32('')
        file_md5 = hashlib.md5()
        file_size = 0
        segments = None
        zfec_padding_size = None
        try:
            # XXX refactor this loop. it's awkward because it needs to know
            # when any given slice is the last slice, so it works an iteration
            # behind, but sometimes sends an empty final slice.
            for slice_item in DataSlicer(req.body_file,
                                    incoming_slice_size,
                                    req.content_length):
                if segments:
                    archiver.archive_slice(
                        segments, zfec_padding_size, _reply_timeout
                    )
                file_adler32 = zlib.adler32(slice_item, file_adler32)
                file_md5.update(slice_item)
                file_size += len(slice_item)
                segments = segmenter.encode(block_generator(slice_item))
                zfec_padding_size = segmenter.padding_size(slice_item)
            archiver.archive_final(
                file_size,
                file_adler32,
                file_md5.digest(),
                segments,
                zfec_padding_size,
                _reply_timeout
            )
        except ArchiveFailedError, instance:
            self._event_push_client.error(
                "archive-failed-error",
                "%s: %s" % (description, instance, )
            )
            self._log.error("archive failed: %s %s" % (
                description, instance, 
            ))
            _send_archive_cancel(
                unified_id, conjoined_part, self._data_writer_clients
            )
            # 2011-09-30 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=503, content_type=None)
            response.retry_after = _archive_retry_interval
            self._stats["archives"] -= 1
            return response
        except Exception, instance:
            # 2012-07-14 dougfort -- were getting
            # IOError: unexpected end of file while reading request
            # if the sender croaks
            self._event_push_client.error(
                "archive-failed-error",
                "%s: %s" % (description, instance, )
            )
            self._log.exception("archive failed: %s %s" % (
                description, instance, 
            ))
            _send_archive_cancel(
                unified_id, conjoined_part, self._data_writer_clients
            )
            response = Response(status=500, content_type=None)
            self._stats["archives"] -= 1
            return response
        
        end_time = time.time()
        self._stats["archives"] -= 1

        self.accounting_client.added(
            collection_entry.collection_id,
            timestamp,
            file_size
        )

        self._event_push_client.info(
            "archive-stats",
            description,
            start_time=start_time,
            end_time=end_time,
            bytes_archived=req.content_length
        )

        response_dict = {
            "version_identifier" : self._id_translator.public_id(unified_id),
        }

        response = Response(content_type=_content_type_json)
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(response_dict, 
                                            sort_keys=True, 
                                            indent=4))
        return response

    def _delete_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = \
                self._authenticator.authenticate(collection_name,
                                                 req)
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        if collection_entry is None:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id_to_delete = None
        if "version_identifier" in req.GET:
            version_identifier = req.GET["version_identifier"]
            version_identifier = urllib.unquote_plus(version_identifier)
            unified_id_to_delete = self._id_translator.internal_id(
                version_identifier
            )

        description = \
            "_delete_key: (%s) %r %r key = %r %s" % (
                collection_entry.collection_id,
                collection_entry.collection_name,
                collection_entry.username,
                key,
                unified_id_to_delete
            )
        self._log.info(description)
        data_writers = _create_data_writers(
            self._event_push_client,
            self._data_writer_clients
        )

        unified_id = self._unified_id_factory.next()
        timestamp = create_timestamp()

        destroyer = Destroyer(
            data_writers,
            collection_entry.collection_id,
            key,
            unified_id_to_delete,
            unified_id,
            timestamp
        )

        try:
            destroyer.destroy(_reply_timeout)
        except DestroyFailedError, instance:            
            self._event_push_client.error(
                "delete-failed-error",
                "%s: %s" % (description, instance, )
            )
            self._log.error("delete failed: %s %s" % (
                description, instance, 
            ))
            # 2009-10-08 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=503, content_type=None)
            response.retry_after = _archive_retry_interval
            return response

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        result_dict = {"success" : True}
        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True, 
                                            indent=4))
        return response

    def _start_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = \
                self._authenticator.authenticate(collection_name,
                                                 req)
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        if collection_entry is None:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        self._log.info(
            "start_conjoined: collection = (%s) %r username = %r key = %r" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            key
        ))

        data_writers = _create_data_writers(
            self._event_push_client,
            # _data_writer_clients are the 0mq clients for each of the nodes in
            # the cluster. They may or may not be connected.
            self._data_writer_clients
        ) 
        unified_id = self._unified_id_factory.next()
        timestamp = create_timestamp()

        try:
            start_conjoined_archive(
                data_writers,
                unified_id,
                collection_entry.collection_id,
                key,
                timestamp
            )
        except ConjoinedFailedError, instance:
            self._event_push_client.error(
                "start-conjoined-failed-error",
                "%s: %s" % (unified_id, instance, )
            )
            self._log.error("start-conjoined failed: %s %s" % (
                unified_id, instance, 
            ))
            # 2012-03-21 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=503, content_type=None)
            response.retry_after = _archive_retry_interval
            return response
        except Exception, instance:
            self._event_push_client.error(
                "start-conjoined-failed-error",
                "%s: %s" % (unified_id, instance, )
            )
            self._log.exception("start-conjoined failed: %s %s" % (
                unified_id, instance, 
            ))
            response = Response(status=500, content_type=None)
            return response

        conjoined_dict = {
            "conjoined_identifier"      : \
                    self._id_translator.public_id(unified_id),
            "key"                       : key,
            "create_timestamp"          : repr(timestamp)   
        }

        response = Response(content_type=_content_type_json)
        # 2012-08-16 dougfort Ticket #29 - set format json for debuging
        response.body_file.write(json.dumps(conjoined_dict, 
                                            sort_keys=True, 
                                            indent=4))
        return response

    def _finish_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")

        try:
            collection_entry = \
                self._authenticator.authenticate(collection_name,
                                                 req)
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        if collection_entry is None:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id = self._id_translator.internal_id(conjoined_identifier)

        self._log.info(
            "finish_conjoined: collection = (%s) %r %r key = %r %s" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            key,
            unified_id
        ))

        data_writers = _create_data_writers(
            self._event_push_client,
            # _data_writer_clients are the 0mq clients for each of the nodes in
            # the cluster. They may or may not be connected.
            self._data_writer_clients
        ) 
        timestamp = create_timestamp()

        try:
            finish_conjoined_archive(
                data_writers,
                collection_entry.collection_id,
                key,
                unified_id,
                timestamp
            )
        except ConjoinedFailedError, instance:
            self._event_push_client.error(
                "finish-conjoined-failed-error",
                "%s: %s" % (unified_id, instance, )
            )
            self._log.error("finish-conjoined failed: %s %s" % (
                unified_id, instance, 
            ))
            # 2012-03-21 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=503, content_type=None)
            response.retry_after = _archive_retry_interval
            return response
        except Exception, instance:
            self._event_push_client.error(
                "finish-conjoined-failed-error",
                "%s: %s" % (unified_id, instance, )
            )
            self._log.exception("finish-conjoined failed: %s %s" % (
                unified_id, instance, 
            ))
            response = Response(status=500, content_type=None)
            return response

        return  Response()

    def _abort_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")

        try:
            collection_entry = \
                self._authenticator.authenticate(collection_name,
                                                 req)
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        if collection_entry is None:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id = self._id_translator.internal_id(conjoined_identifier)

        self._log.info(
            "abort_conjoined: collection = (%s) %r %r key = %r %s" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            key,
            unified_id
        ))

        data_writers = _create_data_writers(
            self._event_push_client,
            # _data_writer_clients are the 0mq clients for each of the nodes in
            # the cluster. They may or may not be connected.
            self._data_writer_clients
        ) 
        timestamp = create_timestamp()

        try:
            abort_conjoined_archive(
                data_writers,
                collection_entry.collection_id,
                key,
                unified_id,
                timestamp
            )
        except ConjoinedFailedError, instance:
            self._event_push_client.error(
                "abort-conjoined-failed-error",
                "%s: %s" % (unified_id, instance, )
            )
            self._log.error("abort-conjoined failed: %s %s" % (
                unified_id, instance, 
            ))
            # 2012-03-21 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=503, content_type=None)
            response.retry_after = _archive_retry_interval
            return response
        except Exception, instance:
            self._event_push_client.error(
                "abort-conjoined-failed-error",
                "%s: %s" % (unified_id, instance, )
            )
            self._log.exception("abort-conjoined failed: %s %s" % (
                unified_id, instance, 
            ))
            response = Response(status=500, content_type=None)
            return response

        return  Response()

