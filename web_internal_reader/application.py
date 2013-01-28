"""
application.py

The nimbus.io wsgi application

for a write:
at startup time, web server creates resilient_client to each node
application:
retrieve:
  ResilientClient, deliver



"""
import httplib
import logging
import os
from itertools import chain
import re
import time
import uuid

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import create_timestamp, \
        block_size

from tools.zfec_segmenter import ZfecSegmenter
from tools.iter_exception_logger import iter_exception_logger

from web_public_reader.retriever import memcached_key_template

from web_internal_reader.exceptions import RetrieveFailedError
from web_internal_reader.retriever import Retriever
from web_internal_reader.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_retrieve_key

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_reply_timeout = float(
    os.environ.get("NIMBUS_IO_REPLY_TIMEOUT",  str(5 * 60.0))
)
_nimbusio_node_name = os.environ['NIMBUSIO_NODE_NAME']
_min_connected_clients = 8
_min_segments = 8
_max_segments = 10

_range_re = re.compile("^bytes=(?P<lower_bound>\d+)-(?P<upper_bound>\d*)$")

def _connected_clients(clients):
    return [client for client in clients if client.connected]

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
    if len(match_object.group("upper_bound")) == 0:
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
        data_readers,
        accounting_client,
        event_push_client,
        stats
    ):
        self._log = logging.getLogger("Application")
        self._memcached_client = memcached_client
        self._central_connection = central_connection
        self._node_local_connection = node_local_connection
        self._cluster_row = cluster_row
        self.data_readers = data_readers
        self.accounting_client = accounting_client
        self._event_push_client = event_push_client
        self._stats = stats


        self._dispatch_table = {
            action_respond_to_ping      : self._respond_to_ping,
            action_retrieve_key         : self._retrieve_key,
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
                    "request {0}: no x-nimbus-io-user-request-id header".format(
                    user_request_id))

        if not action_tag == "respond-to-ping":
            self._log.info("start request {0}: {1} {2}".format( 
                user_request_id, req.method, req.url))

        try:
            return self._dispatch_table[action_tag](req, 
                                                    match_object, 
                                                    user_request_id)
        except exc.HTTPException, instance:
            self._log.error("request {0} {1} {2} {3} {4}".format(
                user_request_id,
                instance.__class__.__name__, 
                instance, 
                action_tag,
                req.url
            ))
            raise
        except Exception, instance:
            self._log.error("exception on request {0} {1}".format( 
                user_request_id, req.url))
            raise

    def _respond_to_ping(self, _req, _match_object, _user_request_id):
        self._log.debug("_respond_to_ping")
        response = Response(status=httplib.OK, content_type="text/plain")
        response.body_file.write("ok")
        return response

    def _get_params_from_memcache(self, unified_id, _conjoined_part):
        """retrieve a cached tuple of (collection_id, key, )"""
        memcached_key = \
            memcached_key_template.format(
                _nimbusio_node_name, unified_id)
        self._log.debug("uncaching {0}".format(memcached_key))
        cached_dict = self._memcached_client.get(memcached_key)

        if cached_dict is None:
            return None

        return (cached_dict["collection-id"], cached_dict["key"], )

    def _get_params_from_database(self, unified_id, conjoined_part):
        return self._node_local_connection.fetch_one_row("""
            select collection_id, key from nimbusio_node.segment
            where unified_id = %s and conjoined_part = %s
            limit 1""", [unified_id, conjoined_part, ])

    def _retrieve_key(self, req, match_object, user_request_id):
        unified_id = int(match_object.group("unified_id"))
        conjoined_part = int(match_object.group("conjoined_part"))

        lower_bound = 0
        upper_bound = None
        slice_offset = 0
        slice_size = None
        total_file_size = None
        if "range" in req.headers:
            lower_bound, upper_bound, slice_offset, slice_size = \
                _parse_range_header(req.headers["range"])
            if not "x-nimbus-io-expected-content-length" in req.headers:
                message = "expected x-nimbus-io-expected-content-length header" 
                self._log.error("request {0} {1}".format(user_request_id, 
                                                         message))
                raise exc.HTTPBadRequest(message)
            total_file_size = \
                int(req.headers["x-nimbus-io-expected-content-length"])

        assert slice_offset % block_size == 0, slice_offset
        block_offset = slice_offset / block_size
        if slice_size is None:
            block_count = None
        else:
            assert slice_size % block_size == 0, slice_size
            block_count = slice_size / block_size

        connected_data_readers = _connected_clients(self.data_readers)

        if len(connected_data_readers) < _min_connected_clients:
            self._log.error("request {0} too few connected readers {1}".format(
                            user_request_id, len(connected_data_readers)))
            raise exc.HTTPServiceUnavailable("Too few connected readers {0}".format(
                len(connected_data_readers)))

        result = self._get_params_from_memcache(unified_id, conjoined_part)
        if result is None:
            self._log.warn("request {0} cache miss unified-id {1} {2}".format(
                user_request_id, unified_id, conjoined_part))
            result = self._get_params_from_database(unified_id, conjoined_part)
        if result is None:
            error_message = "unknown unified-id {0} {1}".format(unified_id,
                                                                conjoined_part)
            self._log.error("request {0} {1}".format(user_request_id, 
                                                     error_message))
            raise exc.HTTPServiceUnavailable(error_message)
        collection_id, key = result

        description = "request {0} retrieve: ({1}) key={2} unified_id={3}-{4} {5}:{6}".format(
            user_request_id,                                                                                        
            collection_id,
            key,
            unified_id,
            conjoined_part,
            slice_offset,
            slice_size
        )
        self._log.info(description)

        start_time = time.time()
        self._stats["retrieves"] += 1

        retriever = Retriever(
            self._node_local_connection,
            self.data_readers,
            collection_id,
            key,
            unified_id,
            conjoined_part,
            block_offset,
            block_count,
            _min_segments,
            user_request_id
        )

        retrieved = retriever.retrieve(_reply_timeout)

        try:
            first_segments = retrieved.next()
        except RetrieveFailedError, instance:
            self._log.error("retrieve failed: {0} {1}".format(
                description, instance
            ))
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

                    for data in data_list:
                        yield data
                        sent += len(data)

            except RetrieveFailedError, instance:
                self._log.error('retrieve failed: {0} {1}'.format(
                    description, instance
                ))
                self._stats["retrieves"] -= 1
                response.status_int = 503
                return

            end_time = time.time()
            self._stats["retrieves"] -= 1

            self.accounting_client.retrieved(
                collection_id,
                create_timestamp(),
                sent
            )

        self._log.info("request {0} successful retrieve".format(user_request_id))

        response_headers = dict()
        if "range" in req.headers:
            status_int = httplib.PARTIAL_CONTENT
            response_headers["Content-Range"] = \
                _content_range_header(lower_bound,
                                      upper_bound,
                                      total_file_size)
            response_headers["Content-Length"] = slice_size
        else:
            status_int = httplib.OK

        response = Response(headers=response_headers)
        
        # Ticket #31 Guess Content-Type and Content-Encoding
        response.content_type = "application/octet-stream"

        response.status_int = status_int

        # the exception blocks below will only catch exceptions before the
        # first yield of the generator, at which point we hand the
        # retrieve_generator off to webob.
        # so, don't let exceptions inside the generator by handled by webob
        # before we get a chance to log them! wrap it in this.
        retrieve_generator = iter_exception_logger(
            "retrieve_generator", 
            "request %s: " % (user_request_id, ),
            app_iterator,
            response)

        response.app_iter = retrieve_generator
        return  response

