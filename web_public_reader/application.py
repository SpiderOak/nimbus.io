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
import itertools
import uuid

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import http_timestamp_str, \
        parse_http_timestamp, \
        create_timestamp
from tools.collection_access_control import read_access, list_access
from tools.interaction_pool_authenticator import AccessUnauthorized, \
        AccessForbidden
from tools.operational_stats_redis_sink import redis_queue_entry_tuple
from tools.iter_exception_logger import iter_exception_logger

from web_public_reader.exceptions import RetrieveFailedError
from web_public_reader.listmatcher import list_keys, list_versions
from web_public_reader.stat_getter import \
    get_last_modified_and_content_length, \
    last_modified_and_content_length_from_key_rows
from web_public_reader.retriever import Retriever
from web_public_reader.meta_manager import retrieve_meta
from web_public_reader.conjoined_manager import list_conjoined_archives, \
        list_upload_in_conjoined
from web_public_reader.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_list_versions, \
        action_list_keys, \
        action_retrieve_meta, \
        action_retrieve_key, \
        action_head_key, \
        action_list_conjoined, \
        action_list_upload_in_conjoined

_reply_timeout = float(
    os.environ.get("NIMBUS_IO_REPLY_TIMEOUT",  str(5 * 60.0))
)

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
        local_interaction_pool,
        cluster_row,
        id_translator,
        authenticator, 
        accounting_client,
        event_push_client,
        redis_queue
    ):
        self._log = logging.getLogger("Application")
        self._interaction_pool = local_interaction_pool
        self._cluster_row = cluster_row
        self._id_translator = id_translator
        self._authenticator = authenticator
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client
        self._redis_queue = redis_queue
        self._request_counter = itertools.count(1)

        self._dispatch_table = {
            action_respond_to_ping      : self._respond_to_ping,
            action_list_versions        : self._list_versions,
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
            user_request_id = req.headers['x-nimbus-io-user-request-id']
        except KeyError:
            user_request_id = str(uuid.uuid4())
            if not action_tag == "respond-to-ping":
                self._log.warn(
                    "request %s: no x-nimbus-io-user-request-id header" % (
                    user_request_id, ) )

        if not action_tag == "respond-to-ping":
            self._log.info("start request %s: %r %r" % ( 
                user_request_id, req.method, req.url, ) )

        try:
            response = self._dispatch_table[action_tag](
                req, match_object, user_request_id)
        except exc.HTTPException, instance:
            self._log.error("request %s %s %s %s %r" % (
                user_request_id,
                instance.__class__.__name__, 
                instance, 
                action_tag,
                req.url
            ))
            raise
        except Exception, instance:
            self._log.exception(instance)
            self._log.error("request %s: exception on %r" 
                % (user_request_id, req.url, ))
            self._event_push_client.exception(
                "unhandled_exception",
                str(instance),
                exctype=instance.__class__.__name__
            )
            raise

        if not action_tag == "respond-to-ping":
            self._log.info(
                "response to request %s: dispatch status_int=%s app_iter=%s"
                % (
                    user_request_id, 
                    str(getattr(response, "status_int", "-")), 
                    str(hasattr(response, "app_iter")), )
            )

        return response

    def _respond_to_ping(self, _req, _match_object, user_request_id):
        # self._log.debug("_respond_to_ping")
        # Ticket #44 We don't send Connection: close here
        # because this is an internal URI
        response = Response(status=httplib.OK, content_type="text/plain")
        response.body_file.write("ok")
        return response

    def _list_versions(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        self._log.debug("request {0}: _list_versions".format(user_request_id))

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 list_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                            user_request_id, instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, instance))
            raise exc.HTTPUnauthorized()
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            raise exc.HTTPBadRequest()
            
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

        self._log.info("request {0}: " \
                       "_list_versions: collection = ({1}) {2} {3} {4}".format(
                       user_request_id,
                       collection_row["id"],
                       collection_row["name"],
                       collection_row["versioning"],
                       kwargs))

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("listmatch_request", queue_entry, ))

        try:
            result_dict = list_versions(self._interaction_pool,
                                        collection_row["id"], 
                                        collection_row["versioning"],
                                        **kwargs)
        # segment_visibility raises ValueError if it is unhappy
        except ValueError, instance:
            self._log.error("request {0}: {1}".format(
                            user_request_id, instance))
            raise exc.HTTPBadRequest(instance)
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("listmatch_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("listmatch_success", queue_entry, ))
        if "content-length" in req.headers:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=req.headers["content-length"])
            self._redis_queue.put(("success_bytes_in_request", queue_entry, ))

        # translate version ids to the form we show to the public
        if "key_data" in result_dict:
            for key_entry in result_dict["key_data"]:
                key_entry["version_identifier"] = \
                    self._id_translator.public_id(
                        key_entry["version_identifier"]
                    )

        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True, 
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))
        return response

    def _list_keys(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        self._log.debug("_list_keys")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 list_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                            user_request_id, instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, instance))
            raise exc.HTTPUnauthorized()
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            raise exc.HTTPBadRequest()
            
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

        self._log.info("request {0}: " \
                       "_list_keys: collection = ({1}) {2} {3} {4}".format(
                       user_request_id,
                       collection_row["id"],
                       collection_row["name"],
                       collection_row["versioning"],
                       kwargs))

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("listmatch_request", queue_entry, ))

        try:
            result_dict = list_keys(self._interaction_pool,
                                    collection_row["id"], 
                                    collection_row["versioning"], 
                                    **kwargs)
        # segment_visibility raises ValueError if it is unhappy
        except ValueError, instance:
            self._log.error("request {0}: {1}".format(user_request_id, instance))
            raise exc.HTTPBadRequest(instance)
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("listmatch_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=1)
        self._redis_queue.put(("listmatch_success", queue_entry, ))
        if "content-length" in req.headers:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=req.headers["content-length"])
            self._redis_queue.put(("success_bytes_in", queue_entry, ))

        # translate version ids to the form we show to the public
        if "key_data" in result_dict:
            for key_entry in result_dict["key_data"]:
                key_entry["version_identifier"] = \
                    self._id_translator.public_id(
                        key_entry["version_identifier"]
                    )

        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        response.body_file.write(json.dumps(result_dict, 
                                            sort_keys=True,
                                            indent=4))

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _retrieve_key(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        self._log.debug("_retrieve_key")

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 read_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                (user_request_id, instance, )))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, 
                            instance))
            raise exc.HTTPUnauthorized()
        except Exception, instance:
            self._log.exception("request {0}: auth exception {1}".format(
                                user_request_id, 
                                instance))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            self._log.exception("request {0}".format(user_request_id))
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

        description = "request {0}: retrieve ({1}) {2} key={3} version={4} {5}:{6}".format(
            user_request_id,
            collection_row["id"],
            collection_row["name"],
            repr(key),
            version_id,
            slice_offset,
            slice_size)
        self._log.info(description)

        # 2012-12-12 dougfort: we handle redis space accounting stats in
        # the Retriever object

        response_headers = dict()
        response = Response(headers=response_headers)

        try:
            retriever = Retriever(
                self._interaction_pool,
                self._redis_queue,
                collection_row["id"],
                collection_row["versioning"],
                key,
                version_id,
                slice_offset,
                slice_size,
                user_request_id
            )

            # the exception blocks below will only catch exceptions before the
            # first yield of the generator, at which point we hand the
            # retrieve_generator off to webob.
            # so, don't let exceptions inside the generator by handled by webob
            # before we get a chance to log them! wrap it in this.
            retrieve_generator = iter_exception_logger(
                "retrieve_generator", 
                "request %s: " % (user_request_id, ),
                retriever.retrieve,
                response, 
                _reply_timeout)

        except RetrieveFailedError, instance:
            self._log.error("retrieve failed: %s %s" % (
                description, instance,
            ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            return exc.HTTPNotFound(str(instance))
        except Exception, instance:
            self._log.exception(
                "retrieve_generator init exception: {0}".format(instance))
            self._event_push_client.exception(
                "unhandled_exception in retrieve",
                str(instance),
                exctype=instance.__class__.__name__
            )
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise

        last_modified, content_length = \
            last_modified_and_content_length_from_key_rows(retriever._key_rows)

        if last_modified is None or content_length is None:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=1)
            self._redis_queue.put(("retrieve_error", queue_entry, ))
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise exc.HTTPNotFound("Not Found: %r" % (key, ))

        # Ticket #31 Guess Content-Type and Content-Encoding
        content_type, content_encoding = \
            mimetypes.guess_type(key, strict=False)

        # Ticket #37 handle If-Modified-Since and If-Unmodified-Since headers

        if "If-Modified-Since" in req.headers:
            timestamp_str = req.headers["If-Modified-Since"]
            try:
                timestamp = parse_http_timestamp(timestamp_str)
            except Exception, instance:
                self._log.error(
                    "unparsable timestamp '{0}'".format(timestamp_str))
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=1)
                self._redis_queue.put(("retrieve_error", queue_entry, ))
                if "content-length" in req.headers:
                    queue_entry = \
                        redis_queue_entry_tuple(timestamp=create_timestamp(),
                                                collection_id=collection_row["id"],
                                                value=req.headers["content-length"])
                    self._redis_queue.put(("error_bytes_in", queue_entry, ))
                raise exc.HTTPServiceUnavailable(str(instance))
            if last_modified < timestamp:
                # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
                response.headers["Connection"] = "close"
                response.last_modified = last_modified
                # 2012-12-12 dougfort: is this a successful retrieve in terms
                # of space accounting?
                if "content-length" in req.headers:
                    queue_entry = \
                        redis_queue_entry_tuple(timestamp=create_timestamp(),
                                                collection_id=collection_row["id"],
                                                value=req.headers["content-length"])
                    self._redis_queue.put(("error_bytes_in", queue_entry, ))
                response.status_int = httplib.NOT_MODIFIED
                return  response

        if "If-Unmodified-Since" in req.headers:
            timestamp_str = req.headers["If-Unmodified-Since"]
            try:
                timestamp = parse_http_timestamp(timestamp_str)
            except Exception, instance:
                self._log.error(
                    "unparsable timestamp '{0}'".format(timestamp_str))
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=1)
                self._redis_queue.put(("retrieve_error", queue_entry, ))
                if "content-length" in req.headers:
                    queue_entry = \
                        redis_queue_entry_tuple(timestamp=create_timestamp(),
                                                collection_id=collection_row["id"],
                                                value=req.headers["content-length"])
                    self._redis_queue.put(("error_bytes_in", queue_entry, ))
                raise exc.HTTPServiceUnavailable(str(instance))
            if last_modified > timestamp:
                # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=1)
                self._redis_queue.put(("retrieve_error", queue_entry, ))
                if "content-length" in req.headers:
                    queue_entry = \
                        redis_queue_entry_tuple(timestamp=create_timestamp(),
                                                collection_id=collection_row["id"],
                                                value=req.headers["content-length"])
                    self._redis_queue.put(("error_bytes_in", queue_entry, ))
                response.headers["Connection"] = "close"
                response.last_modified = last_modified
                response.status_int = httplib.PRECONDITION_FAILED
                return  response

        if "range" in req.headers:
            status_int = httplib.PARTIAL_CONTENT
            response_headers["Content-Range"] = \
                _content_range_header(lower_bound,
                                      upper_bound,
                                      retriever.total_file_size)
            content_length = slice_size
        else:
            status_int = httplib.OK

        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        response.last_modified = last_modified
        response.content_length = content_length

        if content_type is None:
            response.content_type = "application/octet-stream"
        else:
            response.content_type = content_type
        if content_encoding is not None:
            response.content_encoding = content_encoding

        response.status_int = status_int
        response.app_iter = retrieve_generator
        
        return response

    def _retrieve_meta(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        self._log.debug("request {0}: _retrieve_meta".format(user_request_id))

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 None,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                            user_request_id, instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, instance))
            raise exc.HTTPUnauthorized()
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        meta_dict = retrieve_meta(self._interaction_pool, 
                                  collection_row["id"], 
                                  collection_row["versioning"],
                                  key)

        if meta_dict is None:
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise exc.HTTPNotFound(req.url)

        if "content-length" in req.headers:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=req.headers["content-length"])
            self._redis_queue.put(("error_bytes_in", queue_entry, ))

        response = Response(content_type=_content_type_json)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        # 2012-08-16 dougfort Ticket #29 - set format json for debuging
        response.body_file.write(json.dumps(meta_dict, 
                                            sort_keys=True, 
                                            indent=4))

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _head_key(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        self._log.debug("request {0}: _head_key".format(user_request_id))

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 read_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                            user_request_id, instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, instance))
            raise exc.HTTPUnauthorized()
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            raise exc.HTTPBadRequest()
            
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

        self._log.info("request {0}: " \
                       "head_key: collection = ({1}) {2} key = {3} {4}".format(
                       user_request_id,
                       collection_row["id"], 
                       collection_row["name"],
                       key,
                       version_id))

        last_modified, content_length = \
            get_last_modified_and_content_length(self._interaction_pool,
                                                 collection_row["id"],
                                                 collection_row["versioning"],
                                                 key,
                                                 version_id)
        if last_modified is None or content_length is None:
            if "content-length" in req.headers:
                queue_entry = \
                    redis_queue_entry_tuple(timestamp=create_timestamp(),
                                            collection_id=collection_row["id"],
                                            value=req.headers["content-length"])
                self._redis_queue.put(("error_bytes_in", queue_entry, ))
            raise exc.HTTPNotFound("Not Found: %r" % (key, ))

        status = httplib.OK
        if "content-length" in req.headers:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=req.headers["content-length"])
            self._redis_queue.put(("success_bytes_in", queue_entry, ))

        # Ticket #37 handle If-Modified-Since and If-Unmodified-Since headers

        if "If-Modified-Since" in req.headers:
            timestamp_str = req.headers["If-Modified-Since"]
            try:
                timestamp = parse_http_timestamp(timestamp_str)
            except Exception, instance:
                self._log.error("request {0}: " \
                                "unparsable timestamp '{1}'".format(
                                user_request_id,
                                timestamp_str))
                raise exc.HTTPServiceUnavailable(str(instance))
            if last_modified < timestamp:
                status = httplib.NOT_MODIFIED

        if "If-Unmodified-Since" in req.headers:
            timestamp_str = req.headers["If-Unmodified-Since"]
            try:
                timestamp = parse_http_timestamp(timestamp_str)
            except Exception, instance:
                self._log.error("request {0}: " \
                                "unparsable timestamp '{1}'".format(
                                user_request_id,
                                timestamp_str))
                raise exc.HTTPServiceUnavailable(str(instance))
            if last_modified > timestamp:
                status = httplib.PRECONDITION_FAILED

        response = Response(status=status, content_type=None)
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
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

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _list_conjoined(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        self._log.debug("requesrt {0}: _list_conjoined".format(user_request_id))

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 list_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                            user_request_id, instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, instance))
            raise exc.HTTPUnauthorized()
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            raise exc.HTTPBadRequest()
            
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

        self._log.info("request {0}: " \
                       "list_conjoined: collection = ({1}) {2} {3}".format(
                        user_request_id,
                        collection_row["id"], 
                        collection_row["name"],
                        kwargs))

        truncated, conjoined_entries = list_conjoined_archives(
            self._interaction_pool,
            collection_row["id"],
            **kwargs
        )

        if "content-length" in req.headers:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=collection_row["id"],
                                        value=req.headers["content-length"])
            self._redis_queue.put(("success_bytes_in", queue_entry, ))

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
        # 2012-09-06 dougfort Ticket #44 (temporary Connection: close)
        response.headers["Connection"] = "close"
        # 2012-08-16 dougfort Ticket #29 - set format json for debuging
        response.body_file.write(json.dumps(response_dict, 
                                            sort_keys=True,
                                            indent=4))
        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=collection_row["id"],
                                    value=response.headers["content-length"])
        self._redis_queue.put(("success_bytes_out", queue_entry, ))

        return response

    def _list_upload_in_conjoined(self, req, match_object, user_request_id):
        collection_name = match_object.group("collection_name")
        key = match_object.group("key")
        conjoined_identifier = match_object.group("conjoined_identifier")
        self._log.debug("request {0}: _list_upload_in_conjoined".format(
                        user_request_id))

        try:
            collection_row = \
                self._authenticator.authenticate(collection_name,
                                                 list_access,
                                                 req)
        except AccessForbidden, instance:
            self._log.error("request {0}: forbidden {1}".format(
                            user_request_id, instance))
            raise exc.HTTPForbidden()
        except AccessUnauthorized, instance:
            self._log.error("request {0}: unauthorized {1}".format(
                            user_request_id, instance))
            raise exc.HTTPUnauthorized()
        except Exception:
            self._log.exception("request {0}".format(user_request_id))
            raise exc.HTTPBadRequest()
            
        try:
            key = urllib.unquote_plus(key)
            key = key.decode("utf-8")
        except Exception, instance:
            raise exc.HTTPServiceUnavailable(str(instance))

        unified_id = self._id_translator.internal_id(conjoined_identifier)

        self._log.info("request {0}: " \
                       "list_upload: collection = ({1}) {2} key={3} {4}".format(
                       user_request_id,
                       collection_row["id"], 
                       collection_row["name"],
                       key,
                       unified_id))
