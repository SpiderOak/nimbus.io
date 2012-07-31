"""
application.py

The nimbus.io wsgi application

for a write:
at startup time, web server creates resilient_client to each node
application:
retrieve:
  ResilientClient, deliver



"""
from base64 import b64encode
import logging
import os
import json
from itertools import chain
import urllib
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import create_timestamp, \
        segment_status_final

from tools.collection import get_username_and_collection_id, \
        get_collection_id, \
        set_collection_versioning
from tools.zfec_segmenter import ZfecSegmenter

from web_server.exceptions import SpaceAccountingServerDownError, \
        SpaceUsageFailedError, \
        RetrieveFailedError
from web_server.listmatcher import list_keys, list_versions
from web_server.space_usage_getter import SpaceUsageGetter
from web_server.stat_getter import StatGetter
from web_server.retriever import Retriever
from web_server.meta_manager import retrieve_meta
from web_server.conjoined_manager import list_conjoined_archives, \
        list_upload_in_conjoined
from web_server.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_set_versioning, \
        action_list_versions, \
        action_space_usage, \
        action_list_keys, \
        action_retrieve_meta, \
        action_retrieve_key, \
        action_head_key, \
        action_list_conjoined, \
        action_list_upload_in_conjoined

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_reply_timeout = float(
    os.environ.get("NIMBUS_IO_REPLY_TIMEOUT",  str(5 * 60.0))
)
_min_connected_clients = 8
_min_segments = 8
_max_segments = 10

_retrieve_retry_interval = 120
_content_type_json = "application/json"

def _fix_timestamp(timestamp):
    return (None if timestamp is None else repr(timestamp))

def _connected_clients(clients):
    return [client for client in clients if client.connected]

class Application(object):
    def __init__(
        self, 
        central_connection,
        node_local_connection,
        cluster_row,
        id_translator,
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
        self._id_translator = id_translator
        self.data_readers = data_readers
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client
        self._stats = stats


        self._dispatch_table = {
            action_respond_to_ping      : self._respond_to_ping,
            action_set_versioning       : self._set_versioning,
            action_list_versions        : self._list_versions,
            action_space_usage          : self._collection_space_usage,
            action_list_keys            : self._list_keys,
            action_retrieve_meta        : self._retrieve_meta,
            action_retrieve_key         : self._retrieve_key,
            action_head_key             : self._head_key,
            action_list_conjoined       : self._list_conjoined,
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
        response = Response(status=200, content_type="text/plain")
        response.body_file.write("ok")
        return response

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

