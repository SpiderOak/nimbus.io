# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
from base64 import b64encode
import httplib
import logging
import os

import urllib2

import gevent
import gevent.httplib

from tools.data_definitions import block_size, \
        create_timestamp
from tools.operational_stats_redis_sink import redis_queue_entry_tuple
from segment_visibility.sql_factory import version_for_key

from web_public_reader.exceptions import RetrieveFailedError
from web_public_reader.memcached_client import create_memcached_client

memcached_key_template = "internal_read_{0}_{1}"

_web_internal_reader_host = \
    os.environ["NIMBUSIO_WEB_INTERNAL_READER_HOST"]

_web_internal_reader_port = int(
    os.environ["NIMBUSIO_WEB_INTERNAL_READER_PORT"])

# if web internal reader has a cache available, load info for using it instead
# of always talking to the internal reader directly.
_web_internal_reader_cache_port = None
if 'NIMBUSIO_WEB_INTERNAL_READER_CACHE_PORT' in os.environ:
    _web_internal_reader_cache_port = int(
        os.environ['NIMBUSIO_WEB_INTERNAL_READER_CACHE_PORT'])

_web_internal_reader_max_cache_size = 100 * 1024 ** 2
if 'NIMBUSIO_WEB_INTERNAL_READER_MAX_CACHE_SIZE' in os.environ:
    _web_internal_reader_max_cache_size = int(
        os.environ['NIMBUSIO_WEB_INTERNAL_READER_MAX_CACHE_SIZE'])

_nimbusio_node_name = os.environ['NIMBUSIO_NODE_NAME']
_retrieve_retry_interval = 120
_buffer_size = 1024 ** 2

class Retriever(object):
    """retrieves data from web_internal_reader"""
    def __init__(
        self, 
        interaction_pool,
        redis_queue,
        collection_id, 
        versioned,
        key, 
        version_id,
        slice_offset,
        slice_size,
        user_request_id
    ):
        self._log = logging.getLogger("Retriever")
        self._memcached_client = create_memcached_client()
        self._interaction_pool = interaction_pool
        self._redis_queue = redis_queue
        self._collection_id = collection_id
        self._versioned = versioned
        self._key = key
        self._version_id = version_id
        self._slice_offset = slice_offset
        self._slice_size = slice_size
        self._key_rows = self._fetch_key_rows_from_database()
        self.total_file_size = 0
        self.user_request_id = user_request_id

        self._sequence = 0
                
        # if we are looking for a specified slice, we can stop when
        # we find the last block
        self._last_block_in_slice_retrieved = False


    def _fetch_key_rows_from_database(self):
        # TODO: find a non-blocking way to do this
        # TODO: don't just use the local node, it might be wrong
        sql_text = version_for_key(self._collection_id, 
                                   versioned=self._versioned, 
                                   key=self._key,
                                   unified_id=self._version_id)

        args = {"collection_id" : self._collection_id,
                "key"           : self._key, 
                "unified_id"    : self._version_id}

        async_result = \
            self._interaction_pool.run(interaction=sql_text.encode("utf-8"),
                                       interaction_args=args,
                                       pool=_nimbusio_node_name)
        result = async_result.get()

        if len(result) == 0:
            raise RetrieveFailedError("key not found {0} {1} {2}".format(
                self._collection_id, 
                self._key,
                self._version_id))

        # row is of type psycopg2.extras.RealDictRow
        # we want an honest dict
        return [dict(row.items()) for row in result]

    def _cache_key_rows_in_memcached(self, key_rows):
        memcached_key = \
            memcached_key_template.format(_nimbusio_node_name, 
                                          key_rows[0]["unified_id"])
        self._log.debug("request {0}: memcached_key = {1}".format(
                        self.user_request_id,
                        memcached_key))

        # pickle also won't handle the md5 digest, so we encode
        for key_row in key_rows:
            key_row["file_hash"] = b64encode(key_row["file_hash"]) 
            if key_row["combined_hash"] is not None:
                key_row["combined_hash"] = b64encode(key_row["combined_hash"]) 

        cache_dict = {
            "collection-id" : self._collection_id,
            "key"           : self._key,
            "status-rows"   : key_rows,
        }

        self._log.debug("request {0}: caching {1}".format(self.user_request_id, 
                                                          memcached_key))
        try:
            successful = self._memcached_client.set(memcached_key, cache_dict)
        except Exception, instance:
            self._log.exception(instance)
            raise RetrieveFailedError("{0} {1} {2} {3}".format(
                self._collection_id, 
                self._key,
                self._version_id,
                instance))

        if not successful:
            self._log.warn("request {0}: " \
                           "memcached set failed {1}".format(self.user_request_id, 
                                                             memcached_key))

    def _generate_key_rows(self, key_rows):

        # 2012-03-14 dougfort -- note that we are dealing wiht two different
        # types of 'size': 'raw' and 'zfec encoded'. 
        #
        # The caller requests slice_offset and slice_size in 'raw' size, 
        # segment file_size is also 'raw' size.
        #
        # But what we are going to request from each data_writer are 
        # 'zfec encoded' blocks which are smaller than raw blocks
        #
        # And we have to be careful, because each conjoined part could end 
        # with a short block

        # the sum of the sizes of all conjoined files we have looked at
        cumulative_file_size = 0

        # the amount of the slice we have retrieved
        cumulative_slice_size = 0

        # the raw offset adjusted for conjoined files we have skipped
        current_file_offset = None

        # this is how many blocks we skip at the start of this file
        # this applies to both raw blocks here and encoded blocks
        # at the data writers
        block_offset = None

        # number of blocks to retrieve from the current file
        # None means all blocks
        # note that we have to compute this for the last file only
        # but must allow for a possible short block at the end of each 
        # preceding file        
        block_count = None

        # the amount to chop off the front of the first block of a slice
        offset_into_first_block = 0

        # the amount keep from the last block of a slice
        offset_into_last_block = 0

        for key_row in key_rows:

            if self._last_block_in_slice_retrieved:
                break

            next_cumulative_file_size = \
                    cumulative_file_size + key_row["file_size"]
            if next_cumulative_file_size <= self._slice_offset:
                cumulative_file_size = next_cumulative_file_size
                continue

            if current_file_offset is None:
                current_file_offset = \
                        self._slice_offset - cumulative_file_size
                assert current_file_offset >= 0
                
                block_offset = current_file_offset / block_size
                offset_into_first_block = current_file_offset - \
                                          (block_offset * block_size)

            if self._slice_size is not None:
                assert cumulative_slice_size < self._slice_size
                next_slice_size = \
                    cumulative_slice_size + \
                        (key_row["file_size"] - current_file_offset)
                if next_slice_size >= self._slice_size:
                    self._last_block_in_slice_retrieved = True
                    current_file_slice_size = \
                            self._slice_size - cumulative_slice_size
                            
                    block_count = current_file_slice_size / block_size                    
                    slice_remainder = current_file_slice_size % block_size

                    if slice_remainder > 0:
                        offset_into_last_block = slice_remainder
                        block_count += 1

                    # we need to make up for 
                    # the piece we took out of the first block for 
                    # offset_into_first_block
                    offset_into_last_block += offset_into_first_block

                    # if offset_into_first_block is big enough,
                    # we could run over into yet another block
                    if offset_into_last_block >= block_size:
                        block_count += 1
                        offset_into_last_block -= block_size 

                else:
                    cumulative_slice_size = next_slice_size

            self._log.debug("request {0}: "
                            "cumulative_file_size={1}, "
                           "cumulative_slice_size={2}, "
                           "current_file_offset={3}, "
                           "block_offset={4}, "
                           "block_count={5}, "
                           "offset_into_first_block={6}, " 
                           "offset_into_last_block={7}".format(
                           self.user_request_id,
                           cumulative_file_size,
                           cumulative_slice_size,
                           current_file_offset,
                           block_offset,
                           block_count,
                           offset_into_first_block,
                           offset_into_last_block))
                    
            yield key_row, \
                  block_offset, \
                  block_count, \
                  offset_into_first_block, \
                  offset_into_last_block

            cumulative_file_size = next_cumulative_file_size
            current_file_offset = 0
            block_offset = 0
            block_count = None
            offset_into_first_block = 0

    def retrieve(self, response, timeout):
        try:
            return self._retrieve(response, timeout)
        except Exception, instance:
            self._log.error("request {0} _retrieve exception".format(
                self.user_request_id))
            self._log.exception("request {0}".format(self.user_request_id))
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=self._collection_id,
                                        value=1)
            self._redis_queue.put(("retrieve_error", queue_entry, ))
            response.status_int = httplib.SERVICE_UNAVAILABLE
            response.retry_after = _retrieve_retry_interval
            raise RetrieveFailedError(instance)

    def _retrieve(self, response, timeout):
        self._log.debug("request {0}: start _retrieve".format(
            (self.user_request_id)))
        self._cache_key_rows_in_memcached(self._key_rows)
        self.total_file_size = sum([row["file_size"] for row in self._key_rows])
        self._log.debug("total_file_size = {0}".format(self.total_file_size))

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=self._collection_id,
                                    value=1)
        self._redis_queue.put(("retrieve_request", queue_entry, ))
        retrieve_bytes = 0L

        self._log.debug("start key_rows loop")
        first_block = True

        for entry in self._generate_key_rows(self._key_rows):
            key_row, \
            block_offset, \
            block_count, \
            offset_into_first_block, \
            offset_into_last_block = entry

            self._log.debug("request {0}: {1} {2}".format(
                            self.user_request_id,
                            key_row["unified_id"], 
                            key_row["conjoined_part"]))

            # if a cache port is defined, and this response isn't larger than
            # the configured maximum, send the request through the cache.
            target_port = _web_internal_reader_port
            if (_web_internal_reader_cache_port is not None and
                key_row["file_size"] <=
                    _web_internal_reader_max_cache_size 
            ):
                target_port = _web_internal_reader_cache_port

            uri = "http://{0}:{1}/data/{2}/{3}".format(
                _web_internal_reader_host,
                target_port,
                key_row["unified_id"], 
                key_row["conjoined_part"])

            self._log.info(
                "request {0} internally requesting {1}".format(
                self.user_request_id, uri))

            headers = {"x-nimbus-io-user-request-id" : self.user_request_id}

            if block_offset > 0 and block_count is None:
                headers["range"] = \
                    "bytes={0}-".format(block_offset * block_size)
                headers["x-nimbus-io-expected-content-length"] = \
                    str(key_row["file_size"] - (block_offset * block_size))
                expected_status = httplib.PARTIAL_CONTENT
            elif block_count is not None:
                headers["range"] = \
                    "bytes={0}-{1}".format(
                        block_offset * block_size, 
                        (block_offset + block_count) * block_size - 1)
                headers["x-nimbus-io-expected-content-length"] = \
                    str(block_count * block_size)
                expected_status = httplib.PARTIAL_CONTENT
            else:
                headers["x-nimbus-io-expected-content-length"] = \
                            str(key_row["file_size"]),
                expected_status = httplib.OK
                
            request = urllib2.Request(uri, headers=headers)
            self._log.debug(
                "request {0} start internal; expected={1}; headers={2}".format(
                    self.user_request_id, repr(expected_status), headers))
            try:
                urllib_response = urllib2.urlopen(request, timeout=timeout)
            except urllib2.HTTPError, instance:
                if instance.code == httplib.NOT_FOUND:
                    self._log.error(
                        "request {0}: got 404".format(self.user_request_id))
                    response.status_int = httplib.NOT_FOUND
                    break
                if instance.code == httplib.PARTIAL_CONTENT and \
                expected_status ==  httplib.PARTIAL_CONTENT:
                    urllib_response = instance
                else:
                    message = "urllib2.HTTPError '{0}' '{1}'".format(
                        instance.code, instance)
                    self._log.error(
                        "request {0}: exception {1}".format(
                        self.user_request_id, message))
                    self._log.exception(message)
                    response.status_int = httplib.SERVICE_UNAVAILABLE
                    response.retry_after = _retrieve_retry_interval
                    break
            except gevent.httplib.RequestFailed, instance:
                message = "gevent.httplib.RequestFailed '{0}' '{1}'".format(
                    instance.args, instance.message)
                self._log.error(
                    "request {0}: exception {1}".format(
                    self.user_request_id, message))
                self._log.exception(message)
                response.status_int = httplib.SERVICE_UNAVAILABLE
                response.retry_after = _retrieve_retry_interval
                break
            except Exception, instance:
                message = "GET failed {0} '{1}'".format(
                    instance.__class__.__name__, instance)
                self._log.error(
                    "request {0}: exception {1}".format(
                    self.user_request_id, message))
                self._log.exception(message)
                response.status_int = httplib.SERVICE_UNAVAILABLE
                response.retry_after = _retrieve_retry_interval
                break
                
            if urllib_response is None:
                message = "GET returns None {0}".format(uri)
                self._log.error(
                    "request {0}: exception {1}".format(
                    self.user_request_id, message))
                response.status_int = httplib.SERVICE_UNAVAILABLE
                response.retry_after = _retrieve_retry_interval
                break

            self._log.debug(
                "request {0} internal request made".format(
                self.user_request_id))

            # Ticket #68 add buffering
            # 2012-12-23 dougfort -- the choice of block_size as the
            # amount to read is fairly arbitrary but we should read at least 
            # that much
            prev_data = None
            while True:
                data = urllib_response.read(block_size)
                self._log.debug("{0} retrieved {1} bytes from internal".format(
                                self.user_request_id, len(data)))
                if len(data) == 0: 
                    if self._last_block_in_slice_retrieved and \
                    retrieve_bytes >= self._slice_size:
                        self._log.debug("Ending slice at {0}".format(
                                        retrieve_bytes))
                        break
                    assert prev_data is not None
                    if self._last_block_in_slice_retrieved and \
                    offset_into_last_block > 0:
                        # if the first block is the only block, we must
                        # decrement the offset_into_last_block
                        # because we've already chopped offset_into_first_block
                        if first_block:
                            self._log.debug("request {0}: " \
                                            "subtracting {1} bytes from " \
                                            "offset_into_last_block".format(
                                            self.user_request_id,
                                            offset_into_first_block))
                            offset_into_last_block -= offset_into_first_block
                        self._log.debug("request {0}: " \
                                        "len(prev_data) = {1} " \
                                        "offset_into_last_block = {2}".format(
                                        self.user_request_id, 
                                        len(prev_data),
                                        offset_into_last_block))
                        prev_data = prev_data[:offset_into_last_block]
                    self._log.debug(
                        "request {0} yielding {1} bytes from last block".format(
                        self.user_request_id, len(prev_data)))
                    yield prev_data
                    retrieve_bytes += len(prev_data)
                    break
                if prev_data is None:
                    if first_block:
                        assert len(data) > offset_into_first_block, (
                            len(data), offset_into_first_block)
                        prev_data = data[offset_into_first_block:]
                    else:
                        prev_data = data
                    continue
                self._log.debug(
                    "request {0} yielding {1} bytes".format(
                    self.user_request_id, len(prev_data)))
                yield prev_data
                retrieve_bytes += len(prev_data)
                prev_data = data

                first_block = False

            urllib_response.close()

            self._log.debug(
                "request {0} internal request complete".format(
                self.user_request_id))

        # end - for entry in self._generate_key_rows(self._key_rows):

        if response.status_int in [httplib.OK, httplib.PARTIAL_CONTENT, ]:
            redis_entries = [("retrieve_success", 1),
                             ("success_bytes_out", retrieve_bytes)]
        else:
            redis_entries = [("retrieve_error", 1),
                             ("error_bytes_out", retrieve_bytes)]

        timestamp = create_timestamp()
        for key, value in redis_entries:
            queue_entry = \
                redis_queue_entry_tuple(timestamp=timestamp,
                                    collection_id=self._collection_id,
                                    value=value)
            self._redis_queue.put((key, queue_entry, ))

