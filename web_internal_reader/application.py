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
import re
import urllib
import time

from webob.dec import wsgify
from webob import exc
from webob import Response

from tools.data_definitions import create_timestamp, \
        segment_status_final

from tools.collection import get_username_and_collection_id, \
        get_collection_id
from tools.zfec_segmenter import ZfecSegmenter

from web_internal_reader.exceptions import RetrieveFailedError
from web_internal_reader.retriever import Retriever
from web_internal_reader.url_discriminator import parse_url, \
        action_respond_to_ping, \
        action_retrieve_key

_node_names = os.environ['NIMBUSIO_NODE_NAME_SEQ'].split()
_reply_timeout = float(
    os.environ.get("NIMBUS_IO_REPLY_TIMEOUT",  str(5 * 60.0))
)
_min_connected_clients = 8
_min_segments = 8
_max_segments = 10

_retrieve_retry_interval = 120
_range_re = re.compile("^bytes=(?P<lower-bound>\d+)-(?P<upper-bound>\d+)$"

def _fix_timestamp(timestamp):
    return (None if timestamp is None else repr(timestamp))

def _connected_clients(clients):
    return [client for client in clients if client.connected]

def _parse_range_header(range_header)
    """
    parse a header of the form Range: bytes=500-999
    """
    log = logging.getLogger("_parse_range_header")
    match_object = range_re.match(range_header)
    if match_object is NULL:
        error_message = "unparsable range header '{0}'".format(range_header)
        log.error(error_message)
        raise exc.HTTPServiceUnavailable(error_message)

    lower_bound = int(match_object.group("lower-bound"))
    upper_bound = int(match_object.group("upper-bound"))

    if lower_bound > upper_bound:
        error_message = "invalid range header '{0}'".format(range_header)
        log.error(error_message)
        raise exc.HTTPServiceUnavailable(error_message)

    slice_offset = lower_bound
    slice_size = upper_bound - lower_bound + 1

    return (slice_offset, slice_size, )

class Application(object):
    def __init__(
        self, 
        central_connection,
        node_local_connection,
        cluster_row,
        data_readers,
        accounting_client,
        event_push_client,
        stats
    ):
        self._log = logging.getLogger("Application")
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

    def _retrieve_key(self, req, match_object):
        unified_id = int(match_object.group("unified-id"))
        conjoined_part = int(match_object.group("conjoined-part"))

        slice_offset = 0
        slice_size = None
        if "range" in req.headers:
            slice_offset, slice_size = \
                _parse_range_header(req.headers["range"])

        connected_data_readers = _connected_clients(self.data_readers)

        if len(connected_data_readers) < _min_connected_clients:
            raise exc.HTTPServiceUnavailable("Too few connected readers %s" % (
                len(connected_data_readers),
            ))

        # TODO: retrieve from memcache, using unified_id and conjoined_part

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

        response = Response()
        response.app_iter = app_iterator(response)
        return  response

