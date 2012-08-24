# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import httplib
import logging
import os
import urllib2

import gevent

from tools.data_definitions import block_size, \
        segment_status_final, \
        segment_status_cancelled

from web_public_reader.exceptions import RetrieveFailedError
from web_public_reader.local_database_util import current_status_of_key, \
    current_status_of_version

memcached_key_template = "internal_read_{0}"

_web_internal_reader_host = \
    os.environ["NIMBUSIO_WEB_INTERNAL_READER_HOST"]
_web_internal_reader_port = \
    int(os.environ["NIMBUSIO_WEB_INTERNAL_READER_PORT"])

class Retriever(object):
    """retrieves data from web_internal_reader"""
    def __init__(
        self, 
        memcached_client,
        interaction_pool,
        collection_id, 
        key, 
        version_id,
        slice_offset,
        slice_size
    ):
        self._log = logging.getLogger("Retriever")
        self._log.info("{0}, {1}, {2}, {3}, {4}".format(
            collection_id, 
            key, 
            version_id,
            slice_offset,
            slice_size,
        ))
        self._memcached_client = memcached_client
        self._interaction_pool = interaction_pool
        self._collection_id = collection_id
        self._key = key
        self._version_id = version_id
        self._slice_offset = slice_offset
        self._slice_size = slice_size

        self.total_file_size = 0

        self._sequence = 0
                
        # the amount to chop off the front of the first block
        self._offset_into_first_block = 0

        # the amount to chop off the end of the last block
        self._residue_from_last_block = 0

        # if we are looking for a specified slice, we can stop when
        # we find the last block
        self._last_block_in_slice_retrieved = False


    def _fetch_status_rows_from_database(self):
        # TODO: find a non-blocking way to do this
        # TODO: don't just use the local node, it might be wrong
        if self._version_id is None:
            status_rows = current_status_of_key(self._interaction_pool,
                                                self._collection_id, 
                                                self._key)
        else:
            status_rows = current_status_of_version(self._interaction_pool, 
                                                    self._version_id)

        if len(status_rows) == 0:
            raise RetrieveFailedError("key not found %s %s" % (
                self._collection_id, self._key,
            ))

        is_available = False
        if status_rows[0].con_create_timestamp is None:
            is_available = status_rows[0].seg_status == segment_status_final
        else:
            is_available = status_rows[0].con_complete_timestamp is not None

        if not is_available:
            raise RetrieveFailedError("key is not available %s %s" % (
                self._collection_id, self._key,
            ))

        return status_rows

    def _cache_status_rows_in_memcached(self, status_rows):
        memcached_key = \
            memcached_key_template.format(status_rows[0].seg_unified_id)
        cache_dict = {
            "collection-id" : self._collection_id,
            "key"           : self._key,
            "status-rows"   : status_rows,
        }
        self._log.debug("caching {0}".format(memcached_key))
        try:
            self._memcached_client.set(memcached_key, cache_dict)
        except Exception, instance:
            self._log.exception(instance)
            raise

    def _generate_status_rows(self, status_rows):

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

        for status_row in status_rows:

            if status_row.seg_status == segment_status_cancelled:
                continue
                        
            if self._last_block_in_slice_retrieved:
                break

            next_cumulative_file_size = \
                    cumulative_file_size + status_row.seg_file_size
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
                        (status_row.seg_file_size - current_file_offset)
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
                    
            yield status_row, block_offset, block_count

            cumulative_file_size = next_cumulative_file_size
            current_file_offset = 0
            block_offset = 0
            block_count = None

    def retrieve(self, timeout):
        try:
            return self._retrieve(timeout)
        except Exception, instance:
            self._log.exception(instance)
            raise RetrieveFailedError(instance)

    def _retrieve(self, timeout):
        status_rows = self._fetch_status_rows_from_database()
        self._cache_status_rows_in_memcached(status_rows)
        self.total_file_size = sum([r.seg_file_size for r in status_rows])

        self._log.debug("start status_rows loop")
        first_block = True
        for entry in self._generate_status_rows(status_rows):

            status_row, block_offset, block_count = entry

            self._log.debug("request retrieve: {0} {1}".format(
                status_row.seg_unified_id, 
                status_row.seg_conjoined_part
            ))

            uri = "http://{0}:{1}/data/{2}/{3}".format(
                _web_internal_reader_host,
                _web_internal_reader_port,
                status_row.seg_unified_id, 
                status_row.seg_conjoined_part)
            self._log.info("requesting {0}".format(uri))

            headers = {"x-nimbus-io-expected-content-length" : \
                            str(status_row.seg_file_size)}

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
                response = urllib2.urlopen(request, timeout=timeout)
            except urllib2.HTTPError, instance:
                if instance.code == httplib.PARTIAL_CONTENT and \
                expected_status ==  httplib.PARTIAL_CONTENT:
                    response = instance
                else:
                    message = "urllib2.HTTPError '{0}' '{1}'".format(
                        instance.code, instance)
                    self._log.exception(message)
                    raise RetrieveFailedError(message)
            except gevent.httplib.RequestFailed, instance:
                message = "gevent.httplib.RequestFailed '{0}' '{1}'".format(
                    instance.args, instance.message)
                self._log.exception(message)
                raise RetrieveFailedError(message)
            except Exception, instance:
                message = "GET failed {0} '{1}'".format(
                    instance.__class__.__name__, instance)
                self._log.exception(message)
                raise RetrieveFailedError(message)
                
            if response is None:
                message = "GET returns None {0}".format(uri)
                self._log.error(message)
                raise RetrieveFailedError(message)

            # TODO: might be a good idea to buffer here
            data = response.read()
            response.close()
            self._log.debug("retrieved {0} bytes".format(len(data)))

            if first_block:
                data = data[self._offset_into_first_block:]

            if self._last_block_in_slice_retrieved and \
                self._residue_from_last_block > 0:
                data = data[:-self._residue_from_last_block]

            self._log.debug("yielding {0} bytes".format(len(data)))
            yield data

            first_block = False

