# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import logging
import os

import gevent
import gevent.httplib

from tools.data_definitions import block_size, \
        segment_status_final, \
        segment_status_cancelled

from web_server.exceptions import RetrieveFailedError
from web_server.local_database_util import current_status_of_key, \
    current_status_of_version

_web_internal_reader_host = \
    os.environ["NIMBUSIO_WEB_INTERNAL_READER_HOST"]
_web_internal_reader_port = \
    int(os.environ["NIMBUSIO_WEB_INTERNAL_READER_PORT"])

class Retriever(object):
    """retrieves data from web_internal_reader"""
    def __init__(
        self, 
        node_local_connection,
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
        self._node_local_connection = node_local_connection
        self._collection_id = collection_id
        self._key = key
        self._version_id = version_id
        self._slice_offset = slice_offset
        self._slice_size = slice_size
        self._sequence = 0
                
        # the amount to chop off the front of the first block
        self._offset_into_first_block = 0

        # the amount to chop off the end of the last block
        self._residue_from_last_block = 0

        # if we are looking for a specified slice, we can stop when
        # we find the last block
        self._last_block_in_slice_retrieved = False

    @property
    def offset_into_first_block(self):
        return self._offset_into_first_block

    @property
    def residue_from_last_block(self):
        return self._residue_from_last_block

    def _generate_status_rows(self):
        # TODO: find a non-blocking way to do this
        # TODO: don't just use the local node, it might be wrong
        if self._version_id is None:
            status_rows = current_status_of_key(
                self._node_local_connection,
                self._collection_id, 
                self._key,
            )
        else:
            status_rows = current_status_of_version(
                self._node_local_connection, self._version_id
            )

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
                           "block_count={4}".format(cumulative_file_size,
                                                    cumulative_slice_size,
                                                    current_file_offset,
                                                    block_offset,
                                                    block_count))
                    
            yield status_row, block_offset, block_count

            cumulative_file_size = next_cumulative_file_size
            current_file_offset = 0
            block_offset = 0
            block_count = None

    def retrieve(self, timeout):

        http_connection = \
            gevent.httplib.HTTPConnection(_web_internal_reader_host, \
                                          _web_internal_reader_port, 
                                          timeout=timeout)

        for entry in self._generate_status_rows():

            status_row, block_offset, block_count = entry

            self._log.debug("request retrieve: {0} {1}".format(
                status_row.seg_unified_id, 
                status_row.seg_conjoined_part
            ))

            uri = "/".join(["data", 
                            str(status_row.seg_unified_id), 
                            str(status_row.seg_conjoined_part)])
            self._log.info("requesting {0}".format(uri))

            try:
                response = http_connection.request("GET", uri)
            except Exception, instance:
                self._log.exception(str(uri))
                raise

            # TODO: might be a good idea to buffer here
            yield response.read()
            response.close()

        http_connection.close()

