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

from tools.data_definitions import block_size, \
        segment_status_final, \
        segment_status_cancelled, \
        create_timestamp
from tools.operational_stats_redis_sink import redis_queue_entry_tuple
from segment_visibility.sql_factory import version_for_key

from web_public_reader.exceptions import RetrieveFailedError

memcached_key_template = "internal_read_{0}_{1}"

_web_internal_reader_host = \
    os.environ["NIMBUSIO_WEB_INTERNAL_READER_HOST"]
_web_internal_reader_port = \
    int(os.environ["NIMBUSIO_WEB_INTERNAL_READER_PORT"])
_nimbusio_node_name = os.environ['NIMBUSIO_NODE_NAME']
_retrieve_retry_interval = 120
_buffer_size = 1024 ** 2

class Retriever(object):
    """retrieves data from web_internal_reader"""
    def __init__(
        self, 
        memcached_client,
        interaction_pool,
        redis_queue,
        collection_id, 
        versioned,
        key, 
        version_id,
        slice_offset,
        slice_size
    ):
        self._log = logging.getLogger("Retriever")
        self._memcached_client = memcached_client
        self._interaction_pool = interaction_pool
        self._redis_queue = redis_queue
        self._collection_id = collection_id
        self._versioned = versioned
        self._key = key
        self._version_id = version_id
        self._slice_offset = slice_offset
        self._slice_size = slice_size

        self.key_rows = self._fetch_key_rows_from_database()

        self.total_file_size = 0

        self._sequence = 0
                
        # the amount to chop off the front of the first block
        self._offset_into_first_block = 0

        # the amount to chop off the end of the last block
        self._residue_from_last_block = 0

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

        # pickle also won't handle the md5 digest, so we encode
        for key_row in key_rows:
            key_row["file_hash"] = b64encode(key_row["file_hash"]) 
            key_row["combined_hash"] = b64encode(key_row["combined_hash"]) 

        cache_dict = {
            "collection-id" : self._collection_id,
            "key"           : self._key,
            "status-rows"   : key_rows,
        }

        self._log.debug("caching {0}".format(memcached_key))
        try:
            successful = self._memcached_client.set(memcached_key, cache_dict)
        except Exception, instance:
            self._log.exception(instance)
            raise

        if not successful:
            self._log.warn("memcached set failed {0}".format(memcached_key))

    def _generate_key_rows(self, key_rows):

        # 2012-03-14 dougfort -- note that we are dealing wiht two different
        # types of 'size': 'raw' and 'zfec encoded'. 
        #
        # The caller requests slice_offset and slice_size in 'raw' size, 
        # seg_file_size is also 'raw' size.
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

        for key_row in key_rows:

            if key_row["status"] == segment_status_cancelled:
                continue
                        
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
                self._offset_into_first_block = \
                        current_file_offset \
                      - (block_offset * block_size)

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
                    if current_file_slice_size % block_size != 0:
                        block_count += 1
                    self._residue_from_last_block = \
                            (block_count * block_size) - \
                            current_file_slice_size
                    # if we only have a single block, don't take
                    # too big of a chunk out of it
                    if cumulative_slice_size == 0 and block_count == 1:
                        self._residue_from_last_block -= \
                        self._offset_into_first_block
                else:
                    cumulative_slice_size = next_slice_size

            self._log.debug("cumulative_file_size={0}, "
                           "cumulative_slice_size={1}, "
                           "current_file_offset={2}, "
                           "block_offset={3}, "
                           "block_count={4}, "
                           "offset_into_first_block={5}, " 
                           "residue_from_loast_block={6}".format(
                           cumulative_file_size,
                           cumulative_slice_size,
                           current_file_offset,
                           block_offset,
                           block_count,
                           self._offset_into_first_block,
                           self._residue_from_last_block))
                    
            yield key_row, block_offset, block_count

            cumulative_file_size = next_cumulative_file_size
            current_file_offset = 0
            block_offset = 0
            block_count = None

    def retrieve(self, response, timeout):
        try:
            return self._retrieve(response, timeout)
        except Exception, instance:
            self._log.exception(instance)
            queue_entry = \
                redis_queue_entry_tuple(timestamp=create_timestamp(),
                                        collection_id=self._collection_id,
                                        value=1)
            self._redis_queue.put(("retrieve_error", queue_entry, ))
            response.status_int = httplib.SERVICE_UNAVAILABLE
            response.retry_after = _retrieve_retry_interval
            raise RetrieveFailedError(instance)

    def _retrieve(self, response, timeout):
        self._cache_key_rows_in_memcached(self.key_rows)
        self.total_file_size = sum([r.seg_file_size for r in self.key_rows])

        queue_entry = \
            redis_queue_entry_tuple(timestamp=create_timestamp(),
                                    collection_id=self._collection_id,
                                    value=1)
        self._redis_queue.put(("retrieve_request", queue_entry, ))
        retrieve_bytes = 0L

        self._log.debug("start key_rows loop")
        first_block = True
        for entry in self._generate_key_rows(self.key_rows):

            key_row, block_offset, block_count = entry

            self._log.debug("request retrieve: {0} {1}".format(
                key_row["unified_id"], 
                key_row["conjoined_part"]
            ))

            uri = "http://{0}:{1}/data/{2}/{3}".format(
                _web_internal_reader_host,
                _web_internal_reader_port,
                key_row["unified_id"], 
                key_row["conjoined_part"])
            self._log.info("requesting {0}".format(uri))

            headers = {"x-nimbus-io-expected-content-length" : \
                            str(key_row["file_size"])}

            expected_status = httplib.OK
            if block_offset > 0 and block_count is None:
                headers["range"] = \
                    "bytes={0}-".format(block_offset * block_size)
                expected_status = httplib.PARTIAL_CONTENT
            elif block_count is not None:
                headers["range"] = \
                    "bytes={0}-{1}".format(
                        block_offset * block_size, 
                        (block_offset + block_count) * block_size - 1)
                expected_status = httplib.PARTIAL_CONTENT
                
            request = urllib2.Request(uri, headers=headers)
            self._log.debug("start request")
            try:
                urllib_response = urllib2.urlopen(request, timeout=timeout)
            except urllib2.HTTPError, instance:
                if instance.code == httplib.NOT_FOUND:
                    response.status_int = httplib.NOT_FOUND
                    break
                if instance.code == httplib.PARTIAL_CONTENT and \
                expected_status ==  httplib.PARTIAL_CONTENT:
                    urllib_response = instance
                else:
                    message = "urllib2.HTTPError '{0}' '{1}'".format(
                        instance.code, instance)
                    self._log.exception(message)
                    response.status_int = httplib.SERVICE_UNAVAILABLE
                    response.retry_after = _retrieve_retry_interval
                    break
            except gevent.httplib.RequestFailed, instance:
                message = "gevent.httplib.RequestFailed '{0}' '{1}'".format(
                    instance.args, instance.message)
                self._log.exception(message)
                response.status_int = httplib.SERVICE_UNAVAILABLE
                response.retry_after = _retrieve_retry_interval
                break
            except Exception, instance:
                message = "GET failed {0} '{1}'".format(
                    instance.__class__.__name__, instance)
                self._log.exception(message)
                response.status_int = httplib.SERVICE_UNAVAILABLE
                response.retry_after = _retrieve_retry_interval
                break
                
            if urllib_response is None:
                message = "GET returns None {0}".format(uri)
                self._log.error(message)
                response.status_int = httplib.SERVICE_UNAVAILABLE
                response.retry_after = _retrieve_retry_interval
                break

            # Ticket #68 add buffering
            # 2012-12-23 dougfort -- the choice of block_size as the
            # amount to read is fairly arbitrary but we should read at least 
            # that much
            prev_data = None
            while True:
                data = urllib_response.read(block_size)
                if len(data) == 0: 
                    assert prev_data is not None
                    if self._last_block_in_slice_retrieved and \
                    self._residue_from_last_block > 0:
                        prev_data = prev_data[:-self._residue_from_last_block]
                    self._log.debug("yielding {0} bytes from last block".format(
                        len(prev_data)))
                    yield prev_data
                    retrieve_bytes += len(prev_data)
                    break
                if prev_data is None:
                    if first_block:
                        assert len(data) > self._offset_into_first_block
                        prev_data = data[self._offset_into_first_block:]
                    else:
                        prev_data = data
                    continue
                self._log.debug("yielding {0} bytes".format(len(prev_data)))
                yield prev_data
                retrieve_bytes += len(prev_data)
                prev_data = data

            urllib_response.close()
            first_block = False

        # end - for entry in self._generate_key_rows(self.key_rows):

        if response.status_int == httplib.OK:
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

