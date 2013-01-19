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
from base64 import b64decode
import httplib
import logging
import os
import random
import zlib
import hashlib
import json
import urllib
import time

import gevent.greenlet
import gevent.queue

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import incoming_slice_size, \
        block_generator, \
        create_priority, \
        create_timestamp, \
        nimbus_meta_prefix, \
        http_timestamp_str
from tools.collection_access_control import write_access, delete_access
from tools.interaction_pool_authenticator import AccessUnauthorized, \
        AccessForbidden
from tools.operational_stats_redis_sink import redis_queue_entry_tuple

from tools.zfec_segmenter import ZfecSegmenter

from web_writer.exceptions import ArchiveFailedError, \
        DestroyFailedError, \
        ConjoinedFailedError

from web_writer.data_writer_handoff_client import DataWriterHandoffClient
from web_writer.data_writer import DataWriter
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

class ReaderGreenlet(gevent.greenlet.Greenlet):
    def __init__(self, file_object, data_queue):
        gevent.greenlet.Greenlet.__init__(self)
        self._file_object = file_object
        self._data_queue = data_queue

    def _run(self):
        while True:
            data = self._file_object.read(incoming_slice_size)
            if len(data) == 0:
                self._data_queue.put(None)
                break
            self._data_queue.put(data)

class SliceGeneratorTimeout(object):
    pass

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
_max_sequence_upload_interval = 300

def _fix_timestamp(timestamp):
    return (None if timestamp is None else http_timestamp_str(timestamp))

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

def _create_data_writers(clients):
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
        redis_queue
    ):
        self._log = logging.getLogger("Application")
        self._cluster_row = cluster_row
        self._unified_id_factory = unified_id_factory
        self._id_translator = id_translator
        self._data_writer_clients = data_writer_clients
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client
        self._redis_queue = redis_queue


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
        # Ticket #44 we don't send 'Connection: close' here because
        # this is an internal URI
        response = Response(status=httplib.OK, content_type="text/plain")
        response.body_file.write("ok")
        return response

    def _archive_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 write_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("forbidden {0}".format(instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("unauthorized {0}".format(instance))
            raise exc.HTTPUnauthorized()
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.error('unable to prepare key %r %s' % (
                key, instance
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        # Ticket #39 Reject Requests with Inaccurate Content-Length or 
        # Content-Md5 headers 

        if not "content-length" in req.headers:
            raise exc.HTTPLengthRequired()
        try:
            expected_content_length = int(req.headers["content-length"])
        except ValueError:
            error_message = "connot parse content-length {0}".format(
                req.headers["content-length"])
            self._log.error(error_message)
            raise exc.HTTPBadRequest(error_message)

        expected_md5 = None
        if "content-md5" in req.headers:
            expected_md5 = b64decode(req.headers["content-md5"])

        start_time = time.time()
        description = "archive: collection=({0}){1} key={2}, size={3}".format(
            collection_row["id"],
            collection_row["name"],
            key, 
            req.content_length)
        self._log.info(description)

        meta_dict = _build_meta_dict(req.GET)

        unified_id = None
        conjoined_part = 0
        conjoined_archive = "conjoined_identifier" in req.GET

        if conjoined_archive:
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

        data_writers = _create_data_writers(self._data_writer_clients) 
        timestamp = create_timestamp()
        archiver = Archiver(
            data_writers,
            collection_row["id"],
            key,
            unified_id,
            timestamp,
            meta_dict,
            conjoined_part
        )

        if not conjoined_archive:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_request", queue_entry, ))

        data_queue = gevent.queue.Queue()
        reader = ReaderGreenlet(req.body_file, data_queue)
        reader.start()

        segmenter = ZfecSegmenter(_min_segments, len(data_writers))
        actual_content_length = 0
        file_adler32 = zlib.adler32('')
        file_md5 = hashlib.md5()
        file_size = 0
        segments = None
        zfec_padding_size = None
        try:
            while True:
                slice_item = \
                    data_queue.get(block=True, 
                                   timeout=_max_sequence_upload_interval)
                if slice_item is None:
                    break
                actual_content_length += len(slice_item)
                file_adler32 = zlib.adler32(slice_item, file_adler32)
                file_md5.update(slice_item)
                file_size += len(slice_item)
                segments = segmenter.encode(block_generator(slice_item))
                zfec_padding_size = segmenter.padding_size(slice_item)
                if actual_content_length == expected_content_length:
                    archiver.archive_final(
                        file_size,
                        file_adler32,
                        file_md5.digest(),
                        segments,
                        zfec_padding_size,
                        _reply_timeout
                    )
                else:
                    archiver.archive_slice(
                        segments, zfec_padding_size, _reply_timeout
                    )
        except gevent.queue.Empty, instance:
            # Ticket #69 Protection in Web Writer from Slow Uploads
            self._event_push_client.error(
                "archive-failed-error",
                "%s: timeout %s" % (description, instance, )
            )
            self._log.error("archive failed: %s timeout %s" % (
                description, instance, 
            ))
            _send_archive_cancel(
                unified_id, conjoined_part, self._data_writer_clients
            )
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            response = Response(status=httplib.REQUEST_TIMEOUT, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            return response
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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            # 2011-09-30 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=httplib.SERVICE_UNAVAILABLE, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            response.retry_after = _archive_retry_interval
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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            response = Response(status=httplib.INTERNAL_SERVER_ERROR, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            return response

        assert reader.dead
        
        end_time = time.time()

        if actual_content_length != expected_content_length:
            error_message = "actual content length {0} != expected {1}".format(
                actual_content_length, expected_content_length)
            self._log.error(error_message)
            _send_archive_cancel(unified_id, 
                                 conjoined_part, 
                                 self._data_writer_clients)
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=actual_content_length)
            self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise exc.HTTPBadRequest(error_message)

        if expected_md5 is not None and expected_md5 != file_md5.digest():
            error_message = "body md5 does not match content-md5 header"
            self._log.error(error_message)
            _send_archive_cancel(unified_id, 
                                 conjoined_part, 
                                 self._data_writer_clients)
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=actual_content_length)
            self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise exc.HTTPBadRequest(error_message)

        if not conjoined_archive:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_success", queue_entry, ))

        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=actual_content_length)
        self._redis_queue.put(("success_bytes_in", queue_entry, ))

        self.accounting_client.added(
            collection_row["id"],
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
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(response_dict, 
                                            sort_keys=True, 
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))
        return response

    def _delete_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 delete_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("forbidden {0}".format(instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("unauthorized {0}".format(instance))
            raise exc.HTTPUnauthorized()
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
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

        description = "_delete_key: ({0}) {1} key = {2} {3}".format(
            collection_row["id"],
            collection_row["name"],
            key,
            unified_id_to_delete)
        self._log.info(description)
        data_writers = _create_data_writers(self._data_writer_clients)

        unified_id = self._unified_id_factory.next()
        timestamp = create_timestamp()

        destroyer = Destroyer(
            data_writers,
            collection_row["id"],
            key,
            unified_id_to_delete,
            unified_id,
            timestamp
        )

        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("delete_request", queue_entry, ))

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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("delete_error", queue_entry, ))
            # 2011-10-08 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=httplib.SERVICE_UNAVAILABLE, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            response.retry_after = _archive_retry_interval
            return response

        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("delete_success", queue_entry, ))

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        result_dict = {"success" : True}
        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True, 
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _start_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 write_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("forbidden {0}".format(instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("unauthorized {0}".format(instance))
            raise exc.HTTPUnauthorized()
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        self._log.info(
            "start_conjoined: collection = ({0}) {1} key = {2}".format(
            collection_row["id"], 
            collection_row["name"],
            key))

        data_writers = _create_data_writers(self._data_writer_clients) 
        unified_id = self._unified_id_factory.next()
        timestamp = create_timestamp()

        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("archive_request", queue_entry, ))

        try:
            start_conjoined_archive(
                data_writers,
                unified_id,
                collection_row["id"],
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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=timestamp,
                                            collection_id=collection_row["id"],
                                            value=int(req.headers["content-length"]))
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            # 2012-03-21 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=httplib.SERVICE_UNAVAILABLE, 
                                content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=timestamp,
                                            collection_id=collection_row["id"],
                                            value=int(req.headers["content-length"]))
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            response = Response(status=httplib.INTERNAL_SERVER_ERROR, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            return response

        conjoined_dict = {
            "conjoined_identifier"      : \
                    self._id_translator.public_id(unified_id),
            "key"                       : key,
            "create_timestamp"          : _fix_timestamp(timestamp)   
        }

        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        # 2012-08-16 dougfort Ticket #29 - set format json for debuging
        response.body_file.write(json.dumps(conjoined_dict, 
                                            sort_keys=True, 
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _finish_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 write_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("forbidden {0}".format(instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("unauthorized {0}".format(instance))
            raise exc.HTTPUnauthorized()
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id = self._id_translator.internal_id(conjoined_identifier)

        self._log.info(
            "finish_conjoined: collection = ({0}) {1} key = {2} {3}".format(
            collection_row["id"], 
            collection_row["name"],
            key,
            unified_id))

        data_writers = _create_data_writers(self._data_writer_clients) 
        timestamp = create_timestamp()

        try:
            finish_conjoined_archive(
                data_writers,
                collection_row["id"],
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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=timestamp,
                                            collection_id=collection_row["id"],
                                            value=int(req.headers["content-length"]))
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            # 2012-03-21 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=httplib.SERVICE_UNAVAILABLE, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
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
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("archive_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=timestamp,
                                            collection_id=collection_row["id"],
                                            value=int(req.headers["content-length"]))
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            response = Response(status=httplib.INTERNAL_SERVER_ERROR, content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            return response

        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("archive_success", queue_entry, ))
        if "content-length" in req.headers:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                        collection_id=collection_row["id"],
                                        value=int(req.headers["content-length"]))
            self._redis_queue.put(("success_bytes_in", queue_entry, ))

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        result_dict = {"success" : True}
        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True, 
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=int(response.headers["content-length"]))
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _abort_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 write_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("forbidden {0}".format(instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("unauthorized {0}".format(instance))
            raise exc.HTTPUnauthorized()
        except Exception, instance:
            self._log.exception("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id = self._id_translator.internal_id(conjoined_identifier)

        self._log.info(
            "abort_conjoined: collection = ({0}) {1} key = {2} {3}".format(
            collection_row["id"], 
            collection_row["name"],
            key,
            unified_id))

        data_writers = _create_data_writers(self._data_writer_clients) 
        timestamp = create_timestamp()

        try:
            abort_conjoined_archive(
                data_writers,
                collection_row["id"],
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
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=timestamp,
                                            collection_id=collection_row["id"],
                                            value=int(req.headers["content-length"]))
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            # 2012-03-21 dougfort -- assume we have some node trouble
            # tell the customer to retry in a little while
            response = Response(status=httplib.SERVICE_UNAVAILABLE, 
                                content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
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
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=timestamp,
                                            collection_id=collection_row["id"],
                                            value=int(req.headers["content-length"]))
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            response = Response(status=httplib.INTERNAL_SERVER_ERROR, 
                                content_type=None)
            # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
            response.headers["Connection"] = "close"
            return response

        # Ticket #33 Make Nimbus.io API responses consistently JSON
        result_dict = {"success" : True}
        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True, 
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=collection_row["id"],
                                    value=int(response.headers["content-length"]))
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

