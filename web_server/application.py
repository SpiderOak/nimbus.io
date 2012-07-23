"""
application.py

The nimbus.io wsgi application

for a write:
at startup time, web server creates resilient_client to each node
application:
archive:
  at request time, creates DataWriter for each node, regardless of connection
  each DataWriter will have either a ResilientClient for a connected node
   OR will have a HandoffClient which wraps two ResilientClients on behalf of
      a disconnected node
      ResilientClient = tools/greenlet_resilient_client.py
      HandoffClient = web_server/data_writer_handoff_client.py
retrieve:
  ResilientClient, deliver



"""
from base64 import b64encode
import logging
import os
import random
import zlib
import hashlib
import json
from itertools import chain
import urllib
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import incoming_slice_size, \
        block_generator, \
        create_priority, \
        create_timestamp, \
        nimbus_meta_prefix, \
        segment_status_final

from tools.collection import get_username_and_collection_id, \
        get_collection_id, \
        compute_default_collection_name, \
        create_collection, \
        list_collections, \
        delete_collection, \
        set_collection_versioning
from tools.zfec_segmenter import ZfecSegmenter

from web_server.exceptions import SpaceAccountingServerDownError, \
        SpaceUsageFailedError, \
        RetrieveFailedError, \
        ArchiveFailedError, \
        DestroyFailedError, \
        ConjoinedFailedError
from web_server.data_writer_handoff_client import \
        DataWriterHandoffClient
from web_server.data_writer import DataWriter
from web_server.data_slicer import DataSlicer
from web_server.archiver import Archiver
from web_server.destroyer import Destroyer
from web_server.listmatcher import list_keys, list_versions
from web_server.space_usage_getter import SpaceUsageGetter
from web_server.stat_getter import StatGetter
from web_server.retriever import Retriever
from web_server.meta_manager import retrieve_meta
from web_server.conjoined_manager import list_conjoined_archives, \
        start_conjoined_archive, \
        abort_conjoined_archive, \
        finish_conjoined_archive, \
        list_upload_in_conjoined
from web_server.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_list_collections, \
        action_create_collection, \
        action_delete_collection, \
        action_set_versioning, \
        action_list_versions, \
        action_space_usage, \
        action_archive_key, \
        action_list_keys, \
        action_retrieve_meta, \
        action_retrieve_key, \
        action_delete_key, \
        action_head_key, \
        action_list_conjoined, \
        action_start_conjoined, \
        action_finish_conjoined, \
        action_abort_conjoined, \
        action_list_upload_in_conjoined

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
_retrieve_retry_interval = 120
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
        central_connection,
        node_local_connection,
        cluster_row,
        unified_id_factory,
        id_translator,
        data_writer_clients, 
        data_readers,
        authenticator, 
        accounting_client,
        event_push_client,
        stats
    ):
        self._log = logging.getLogger("Application")
        self._central_connection = central_connection
        self._node_local_connection = node_local_connection
        self._cluster_row = cluster_row
        self._unified_id_factory = unified_id_factory
        self._id_translator = id_translator
        self._data_writer_clients = data_writer_clients
        self.data_readers = data_readers
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client
        self._stats = stats


        self._dispatch_table = {
            action_respond_to_ping      : self._respond_to_ping,
            action_list_collections     : self._list_collections,
            action_create_collection    : self._create_collection,
            action_delete_collection    : self._delete_collection,
            action_set_versioning       : self._set_versioning,
            action_list_versions        : self._list_versions,
            action_space_usage          : self._collection_space_usage,
            action_archive_key          : self._archive_key,
            action_list_keys            : self._list_keys,
            action_retrieve_meta        : self._retrieve_meta,
            action_retrieve_key         : self._retrieve_key,
            action_delete_key           : self._delete_key,
            action_head_key             : self._head_key,
            action_list_conjoined       : self._list_conjoined,
            action_start_conjoined      : self._start_conjoined,
            action_finish_conjoined     : self._finish_conjoined,
            action_abort_conjoined      : self._abort_conjoined,
            action_list_upload_in_conjoined : self._list_upload_in_conjoined,
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
        response = Response(status=200, content_type=None)
        return response

    def _list_collections(self, req, match_object):
        username = match_object.group("username")
        self._log.info("_list_collections %r" % (username, ))

        authenticated = self._authenticator.authenticate(
            self._central_connection,
            username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        try:
            collections = list_collections(
                self._central_connection,
                username
            )
        except Exception, instance:
            self._log.error("%r error listing collections %s" % (
                username, instance,
            ))
            raise exc.HTTPServiceUnavailable(str(instance))

        # json won't dump datetime
        json_collections = [
            {"name" : n, "versioning" : v, "creation-time" : t.isoformat()} \
            for (n, v, t) in collections]

        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(json_collections))

        return response

    def _create_collection(self, req, match_object):
        username = match_object.group("username")
        collection_name = match_object.group("collection_name")
        versioning = False

        self._log.info("_create_collection: %s name = %r" % (
            username,
            collection_name,
        ))

        authenticated = self._authenticator.authenticate(
            self._central_connection,
            username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        self._central_connection.begin_transaction()
        try:
            creation_time = create_collection(
                self._central_connection, 
                username,
                collection_name,
                versioning
            )
        except Exception, instance:
            self._log.error("%s error adding collection %r %s" % (
                username, 
                collection_name, 
                instance,
            ))
            self._central_connection.rollback()
            raise exc.HTTPServiceUnavailable(str(instance))
        else:
            self._central_connection.commit()

        # this is the same format returned by list_collection
        collection_dict = {
            "name" : collection_name,
            "versioning" : versioning,
            "creation-time" : creation_time.isoformat()} 

        # 2012-04-15 dougfort Ticket #12 - return 201 'created'
        response = Response(status=201, content_type=_content_type_json)
        response.body_file.write(json.dumps(collection_dict))
        return response

    def _delete_collection(self, req, match_object):
        username = match_object.group("username")
        collection_name = match_object.group("collection_name")

        self._log.info("_delete_collection: %r %r" % (
            username, collection_name, 
        ))

        authenticated = self._authenticator.authenticate(
            self._central_connection,
            username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        # you can't delete your default collection
        default_collection_name = compute_default_collection_name(username)
        if collection_name == default_collection_name:
            raise exc.HTTPForbidden("Can't delete default collection %r" % (
                collection_name,
            ))

        # TODO: can't delete a collection that contains keys
        self._central_connection.begin_transaction()
        try:
            delete_collection(self._central_connection, collection_name)
        except Exception, instance:
            self._log.error("%r %r error deleting collection %s" % (
                username, collection_name, instance,
            ))
            self._central_connection.rollback()
            raise exc.HTTPServiceUnavailable(str(instance))
        else:
            self._central_connection.commit()

        return Response('OK')

    def _set_versioning(self, req, match_object):
        collection_name = match_object.group("collection_name")
        versioning = match_object.group("versioning").lower() == "true"

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()


        set_collection_versioning(
            self._central_connection, collection_name, versioning
        )

        return Response('OK')

    def _list_versions(self, req, match_object):
        collection_name = match_object.group("collection_name")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        variable_names = [
            "prefix",
            "max_keys",
            "delimiter",
            "key_marker",
            "version_id_marker",
        ]

        # pass on any variable names we recognize as keyword args
        kwargs = dict()
        for variable_name in variable_names:
            if variable_name in req.GET:
                variable_value = req.GET[variable_name]
                variable_value = urllib.unquote_plus(variable_value)
                variable_value = variable_value.decode("utf-8")
                kwargs[variable_name] = variable_value

        # translate version id to the form we use internally
        if "version_id_marker" in kwargs:
            kwargs["version_id_marker"] = self._id_translator.internal_id(
                kwargs["version_id_marker"]
            )

        self._log.info(
            "_list_versions: collection = (%s) username = %r %r %s" % (
                collection_entry.collection_id,
                collection_entry.collection_name,
                collection_entry.username,
                kwargs
            )
        )
        result_dict = list_versions(
            self._node_local_connection,
            collection_entry.collection_id, 
            **kwargs
        )

        # translate version ids to the form we show to the public
        if "key_data" in result_dict:
            for key_entry in result_dict["key_data"]:
                key_entry["version_identifier"] = \
                    self._id_translator.public_id(
                        key_entry["version_identifier"]
                    )

        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(result_dict))
        return response

    def _collection_space_usage(self, req, match_object):
        username = match_object.group("username")
        collection_name = match_object.group("collection_name")

        self._log.info("_collection_space_usage: %r %r" % (
            username, collection_name
        ))

        authenticated = self._authenticator.authenticate(
            self._central_connection,
            username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        collection_id = get_collection_id(
            self._central_connection, collection_name
        )        
        if collection_id is None:
            raise exc.HTTPNotFound(collection_name)

        getter = SpaceUsageGetter(self.accounting_client)
        try:
            usage = getter.get_space_usage(collection_id, _reply_timeout)
        except (SpaceAccountingServerDownError, SpaceUsageFailedError), e:
            raise exc.HTTPServiceUnavailable(str(e))

        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(usage))
        return response

    def _archive_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
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
        response.body_file.write(json.dumps(response_dict))
        return response

    def _list_keys(self, req, match_object):
        collection_name = match_object.group("collection_name")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        variable_names = [
            "prefix",
            "max_keys",
            "delimiter",
            "marker"
        ]

        # pass on any variable names we recognize as keyword args
        kwargs = dict()
        for variable_name in variable_names:
            if variable_name in req.GET:
                variable_value = req.GET[variable_name]
                variable_value = urllib.unquote_plus(variable_value)
                variable_value = variable_value.decode("utf-8")
                kwargs[variable_name] = variable_value

        self._log.info(
            "_list_keys: collection = (%s) username = %r %r %s" % (
                collection_entry.collection_id,
                collection_entry.collection_name,
                collection_entry.username,
                kwargs
            )
        )
        result_dict = list_keys(
            self._node_local_connection,
            collection_entry.collection_id, 
            **kwargs
        )

        # translate version ids to the form we show to the public
        if "key_data" in result_dict:
            for key_entry in result_dict["key_data"]:
                key_entry["version_identifier"] = \
                    self._id_translator.public_id(
                        key_entry["version_identifier"]
                    )

        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(result_dict))
        return response

    def _retrieve_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))


        version_id = None
        if "version_identifier" in req.GET:
            version_identifier = req.GET["version_identifier"]
            version_identifier = urllib.unquote_plus(version_identifier)
            version_id = self._id_translator.internal_id(version_identifier)

        slice_offset = 0
        if "slice_offset" in req.GET:
            slice_offset_str = req.GET["slice_offset"]
            try:
                slice_offset = int(slice_offset_str)
            except ValueError:
                self._log.error("invalid slice_offset %r" % (slice_offset_str))
                raise exc.HTTPServiceUnavailable(str(instance))

        slice_size = None
        if "slice_size" in req.GET:
            slice_size_str = req.GET["slice_size"]
            try:
                slice_size = int(slice_size_str)
            except ValueError:
                self._log.error("invalid slice_size %r" % (slice_size_str))
                raise exc.HTTPServiceUnavailable(str(instance))

        connected_data_readers = _connected_clients(self.data_readers)

        if len(connected_data_readers) < _min_connected_clients:
            raise exc.HTTPServiceUnavailable("Too few connected readers %s" % (
                len(connected_data_readers),
            ))

        description = "retrieve: (%s)%r %r key=%r version=%r %r:%r" % (
            collection_entry.collection_id,
            collection_entry.collection_name,
            collection_entry.username,
            key,
            version_id,
            slice_offset,
            slice_size
        )
        self._log.info(description)

        start_time = time.time()
        self._stats["retrieves"] += 1

        retriever = Retriever(
            self._node_local_connection,
            self.data_readers,
            collection_entry.collection_id,
            key,
            version_id,
            slice_offset,
            slice_size,
            _min_segments
        )

        retrieved = retriever.retrieve(_reply_timeout)

        try:
            first_segments = retrieved.next()
        except RetrieveFailedError, instance:
            self._log.error("retrieve failed: %s %s" % (
                description, instance,
            ))
            self._event_push_client.warn(
                "retrieve-failed",
                "%s: %s" % (description, instance, )
            )
            self._stats["retrieves"] -= 1
            return exc.HTTPNotFound(str(instance))

        def app_iterator(response):
            segmenter = ZfecSegmenter( _min_segments, _max_segments)
            sent = 0
            try:
                for segments in chain([first_segments], retrieved):
                    segment_numbers = segments.keys()
                    encoded_segments = list()
                    zfec_padding_size = None
                    for segment_number in segment_numbers:
                        encoded_segment, zfec_padding_size = \
                                segments[segment_number]
                        encoded_segments.append(encoded_segment)
                    data_list = segmenter.decode(
                        encoded_segments,
                        segment_numbers,
                        zfec_padding_size
                    )
                    if sent == 0:
                        data_list[0] = \
                            data_list[0][retriever.offset_into_first_block:]
                    if retriever.residue_from_last_block != 0:
                        data_list[-1] = \
                            data_list[-1][:-retriever.residue_from_last_block]

                    for data in data_list:
                        yield data
                        sent += len(data)
            except RetrieveFailedError, instance:
                self._event_push_client.warn(
                    "retrieve-failed",
                    "%s: %s" % (description, instance, )
                )
                self._log.error('retrieve failed: %s %s' % (
                    description, instance
                ))
                self._stats["retrieves"] -= 1
                response.status_int = 503
                response.retry_after = _retrieve_retry_interval
                return

            end_time = time.time()
            self._stats["retrieves"] -= 1

            self.accounting_client.retrieved(
                collection_entry.collection_id,
                create_timestamp(),
                sent
            )

            self._event_push_client.info(
                "retrieve-stats",
                description,
                start_time=start_time,
                end_time=end_time,
                bytes_retrieved=sent
            )

        # 2011-10-05 dougfort -- going thrpough this convoluted process 
        # to return 503 if the app_iter fails. Instead of rasing an 
        # exception that chokes the customer's retrieve
        # IMO this really sucks 
        response = Response()
        response.app_iter = app_iterator(response)
        return  response

    def _retrieve_meta(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        meta_dict = retrieve_meta(
            self._node_local_connection, 
            collection_entry.collection_id, 
            key
        )

        if meta_dict is None:
            raise exc.HTTPNotFound(req.url)

        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(meta_dict))
        return response

    def _delete_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
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
            self._node_local_connection,
            data_writers,
            collection_entry.collection_id,
            key,
            unified_id_to_delete,
            unified_id,
            timestamp
        )

        try:
            size_deleted = destroyer.destroy(_reply_timeout)
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

        self.accounting_client.removed(
            collection_entry.collection_id,
            timestamp,
            size_deleted
        )
        return Response('OK')

    def _head_key(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        version_id = None
        if "version_identifier" in req.GET:
            version_identifier = req.GET["version_identifier"]
            version_identifier = urllib.unquote_plus(version_identifier)
            version_id = self._id_translator.internal_id(version_identifier)

        self._log.info(
            "head_key: collection = (%s) %r username = %r key = %r %r" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            key,
            version_id
        ))

        getter = StatGetter(self._node_local_connection)
        status_rows = getter.stat(
            collection_entry.collection_id, key, version_id
        )
        if len(status_rows) == 0 or \
           status_rows[0].seg_status != segment_status_final:
            raise exc.HTTPNotFound("Not Found: %r" % (key, ))

        response = Response(status=200, content_type=None)
        response.content_length = sum([r.seg_file_size for r in status_rows])

        if status_rows[0].con_create_timestamp is None:
            response.content_md5 = b64encode(status_rows[0].seg_file_hash)
        else:
            response.content_md5 = None

        return response

    def _list_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        variable_names = [
            "max_conjoined",
            "key_marker",
            "conjoined_identifier_marker"
        ]

        # pass on any variable names we recognize as keyword args
        kwargs = dict()
        for variable_name in variable_names:
            if variable_name in req.GET:
                variable_value = req.GET[variable_name]
                variable_value = urllib.unquote_plus(variable_value)
                variable_value = variable_value.decode("utf-8")
                kwargs[variable_name] = variable_value

        self._log.info(
            "list_conjoined: collection = (%s) %r username = %r %s" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            kwargs,
        ))

        truncated, conjoined_entries = list_conjoined_archives(
            self._node_local_connection,
            collection_entry.collection_id,
            **kwargs
        )

        conjoined_list = list()
        for entry in conjoined_entries:
            row_dict = {
                "conjoined_identifier" : \
                    self._id_translator.public_id(entry.unified_id),
                "key" : entry.key,
                "create_timestamp" : _fix_timestamp(entry.create_timestamp),
                "abort_timestamp"  : _fix_timestamp(entry.abort_timestamp),
                "complete_timestamp":_fix_timestamp(entry.complete_timestamp),
            }
            conjoined_list.append(row_dict)

        response_dict = {
            "conjoined_list" : conjoined_list, 
            "truncated" : truncated
        }

        response = Response(content_type=_content_type_json)
        response.body_file.write(json.dumps(response_dict))
        return response

    def _start_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
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
        response.body_file.write(json.dumps(conjoined_dict))
        return response

    def _finish_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
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
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
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

    def _list_upload_in_conjoined(self, req, match_object):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")

        try:
            collection_entry = get_username_and_collection_id(
                self._central_connection, collection_name
            )
        except Exception, instance:
            self._log.error("%s" % (instance, ))
            raise exc.HTTPBadRequest()
            
        authenticated = self._authenticator.authenticate(
            self._central_connection,
            collection_entry.username,
            req
        )
        if not authenticated:
            raise exc.HTTPUnauthorized()

        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id = self._id_translator.internal_id(conjoined_identifier)

        self._log.info("list_upload: collection = (%s) %r %r key=%r %r" % (
            collection_entry.collection_id, 
            collection_entry.collection_name,
            collection_entry.username,
            key,
            unified_id
        ))

