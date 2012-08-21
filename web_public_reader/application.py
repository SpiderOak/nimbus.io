"""
application.py

The nimbus.io wsgi application

"""
import httplib
import logging
import mimetypes
import os
import re
import json
import urllib

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.collection import get_username_and_collection_id, \
        get_collection_id
from tools.data_definitions import http_timestamp_str

from web_public_reader.exceptions import SpaceAccountingServerDownError, \
        SpaceUsageFailedError
from web_public_reader.listmatcher import list_keys, list_versions
from web_public_reader.space_usage_getter import SpaceUsageGetter
from web_public_reader.stat_getter import get_last_modified_and_content_length
from web_public_reader.retriever import Retriever
from web_public_reader.meta_manager import retrieve_meta
from web_public_reader.conjoined_manager import list_conjoined_archives, \
        list_upload_in_conjoined
from web_public_reader.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_list_versions, \
        action_space_usage, \
        action_list_keys, \
        action_retrieve_meta, \
        action_retrieve_key, \
        action_head_key, \
        action_list_conjoined, \
        action_list_upload_in_conjoined

_reply_timeout = float(
    os.environ.get("NIMBUS_IO_REPLY_TIMEOUT",  str(5 * 60.0))
)

_retrieve_retry_interval = 120
_content_type_json = "application/json"
_range_re = re.compile("^bytes=(?P<lower_bound>\d+)-(?P<upper_bound>\d*)$")

def _fix_timestamp(timestamp):
    return (None if timestamp is None else http_timestamp_str(timestamp))

def _parse_range_header(range_header):
    """
    parse a header of the form Range: bytes=500-999
    """
    log = logging.getLogger("_parse_range_header")
    match_object = _range_re.match(range_header)
    if match_object is None:
        error_message = "unparsable range header '{0}'".format(range_header)
        log.error(error_message)
        raise exc.HTTPServiceUnavailable(error_message)

    lower_bound = int(match_object.group("lower_bound"))
    if len (match_object.group("upper_bound")) == 0:
        upper_bound = None
    else:
        upper_bound = int(match_object.group("upper_bound"))

    if upper_bound is not None and lower_bound > upper_bound:
        error_message = "invalid range header '{0}'".format(range_header)
        log.error(error_message)
        raise exc.HTTPServiceUnavailable(error_message)

    slice_offset = lower_bound
    if upper_bound is None:
        slice_size = None
    else:
        slice_size = upper_bound - lower_bound + 1

    return (lower_bound, upper_bound, slice_offset, slice_size, )

def _content_range_header(lower_bound, upper_bound, total_file_size):
    if upper_bound is None:
        upper_bound = total_file_size - 1
    return "bytes {0}-{1}/{2}".format(lower_bound, 
                                      upper_bound, 
                                      total_file_size)

class Application(object):
    def __init__(
        self, 
        memcached_client,
        central_connection,
        node_local_connection,
        cluster_row,
        id_translator,
        authenticator, 
        accounting_client,
        event_push_client
    ):
        self._log = logging.getLogger("Application")
        self._memcached_client = memcached_client
        self._central_connection = central_connection
        self._node_local_connection = node_local_connection
        self._cluster_row = cluster_row
        self._id_translator = id_translator
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client

        self._dispatch_table = {
            action_respond_to_ping      : self._respond_to_ping,
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
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True, 
                                            indent=4))
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
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(usage, sort_keys=True, indent=4))
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
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True,
                                            indent=4))
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

        lower_bound = 0
        upper_bound = None
        slice_offset = 0
        slice_size = None
        if "range" in req.headers:
            lower_bound, upper_bound, slice_offset, slice_size = \
                _parse_range_header(req.headers["range"])

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

        retriever = Retriever(
            self._memcached_client,
            self._node_local_connection,
            collection_entry.collection_id,
            key,
            version_id,
            slice_offset,
            slice_size
        )

        try:
            retrieve_generator = retriever.retrieve(_reply_timeout)
        except Exception, instance:
            self._log.exception("retrieve_failed {0}".format(instance))
            self._event_push_client.exception(
                "unhandled_exception in retrieve",
                str(instance),
                exctype=instance.__class__.__name__
            )
            raise

        last_modified, content_length = \
            get_last_modified_and_content_length(self._node_local_connection,
                                                 collection_entry.collection_id,
                                                 key,
                                                 version_id)

        if last_modified is None or content_length is None:
            raise exc.HTTPNotFound("Not Found: %r" % (key, ))

        response_headers = dict()
        if "range" in req.headers:
            status_int = httplib.PARTIAL_CONTENT
            response_headers["Content-Range"] = \
                _content_range_header(lower_bound,
                                      upper_bound,
                                      retriever.total_file_size)
            content_length = slice_size
        else:
            status_int = httplib.OK

        response = Response(headers=response_headers)
        response.last_modified = last_modified
        response.content_length = content_length

        # Ticket #31 Guess Content-Type and Content-Encoding
        content_type, content_encoding = \
            mimetypes.guess_type(key, strict=False)
        if content_type is None:
            response.content_type = "application/octet-stream"
        else:
            response.content_type = content_type
        if content_encoding is not None:
            response.content_encoding = content_encoding

        response.status_int = status_int
        response.app_iter = retrieve_generator
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
        # 2012-08-16 dougfort Ticket #29 - set format json for debuging
        response.body_file.write(json.dumps(meta_dict, 
                                            sort_keys=True, 
                                            indent=4))
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

        last_modified, content_length = \
            get_last_modified_and_content_length(self._node_local_connection,
                                                 collection_entry.collection_id,
                                                 key,
                                                 version_id)
        if last_modified is None or content_length is None:
            raise exc.HTTPNotFound("Not Found: %r" % (key, ))

        response = Response(status=200, content_type=None)
        response.last_modified = last_modified
        response.content_length = content_length

        # Ticket #31 Guess Content-Type and Content-Encoding
        content_type, content_encoding = \
            mimetypes.guess_type(key, strict=False)
        if content_type is None:
            response.content_type = "application/octet-stream"
        else:
            response.content_type = content_type
        if content_encoding is not None:
            response.content_encoding = content_encoding

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
        # 2012-08-16 dougfort Ticket #29 - set format json for debuging
        response.body_file.write(json.dumps(response_dict, 
                                            sort_keys=True,
                                            indent=4))
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

