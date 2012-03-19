# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import logging
import time

import gevent
import gevent.pool
import gevent.queue

from tools.data_definitions import block_size, segment_status_final

from web_server.exceptions import RetrieveFailedError
from web_server.local_database_util import current_status_of_key, \
    current_status_of_version

_task_timeout = 60.0

class Retriever(object):
    """Retrieves data from data readers."""
    def __init__(
        self, 
        node_local_connection,
        data_readers, 
        collection_id, 
        key, 
        version_id,
        slice_offset,
        slice_size,
        segments_needed
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
        self._data_readers = data_readers
        self._collection_id = collection_id
        self._key = key
        self._version_id = version_id
        self._slice_offset = slice_offset
        self._slice_size = slice_size
        self._segments_needed = segments_needed
        self._pending = gevent.pool.Group()
        self._finished_tasks = gevent.queue.Queue()
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

    def _done_link(self, task):
        if task.sequence != self._sequence:
            self._log.debug("_done_link ignore task %s seq %s expect %s" % (
                task.data_reader.node_name,
                task.sequence,
                self._sequence
            ))
        else:
            self._finished_tasks.put(task, block=True)

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

        for entry in self._generate_status_rows():

            status_row, block_offset, block_count = entry

            # spawn retrieve_key start, then spawn retrieve key next
            # until we are done
            start = True
            while True:
                self._sequence += 1
                self._log.debug("retrieve: %s %s %s" % (
                    self._sequence, 
                    status_row.seg_unified_id, 
                    status_row.seg_conjoined_part,
                ))
                # send a request to all node
                for i, data_reader in enumerate(self._data_readers):
                    if not data_reader.connected:
                        self._log.warn("ignoring disconnected reader %s" % (
                            str(data_reader),
                        ))
                        continue

                    segment_number = i + 1
                    if start:
                        task = self._pending.spawn(
                            data_reader.retrieve_key_start,
                            status_row.seg_unified_id,
                            status_row.seg_conjoined_part,
                            segment_number,
                            block_offset,
                            block_count
                        )
                    else:
                        task = self._pending.spawn(
                            data_reader.retrieve_key_next,
                            status_row.seg_unified_id,
                            status_row.seg_conjoined_part,
                            segment_number,
                            block_offset,
                            block_count
                        )
                    task.link(self._done_link)
                    task.segment_number = segment_number
                    task.data_reader = data_reader
                    task.sequence = self._sequence

                # wait for, and process, replies from the nodes
                result_dict, completed = self._process_node_replies(timeout)
                self._log.debug("retrieve: completed sequence %s" % (
                    self._sequence,
                ))

                yield result_dict
                if completed:
                    break


                start = False


    def _process_node_replies(self, timeout):
        finished_task_count = 0
        result_dict = dict()
        completed_list = list()
        start_time = time.time()

        # block on the finished_tasks queue until done
        while finished_task_count < len(self._data_readers):
            try:
                task = self._finished_tasks.get(
                    block=True, timeout=_task_timeout
                )
            except gevent.queue.Empty:
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout:
                    error_message = \
                        "timed out _finished_tasks %s %s" % (
                            self._collection_id,
                            self._key,
                        )
                    self._log.error(error_message)
                    raise RetrieveFailedError(error_message)

                self._log.warn("timeout waiting for completed task")
                continue

            # if we previously only waited for 8/10 replies, we may still get
            # those other 2 replies coming in even though we have moved on.
            if task.sequence != self._sequence:
                self._log.debug(
                    "_process_node_replies ignore task %s seq %s expect %s" % (
                        task.data_reader.node_name,
                        task.sequence,
                        self._sequence
                    )
                )
                continue

            finished_task_count += 1
            result = self._process_finished_task(task)

            if result is None:
                continue

            data_segment, zfec_padding_size, completion_status = result

            result_dict[task.segment_number] = \
                    (data_segment, zfec_padding_size, )
            completed_list.append(completion_status)

            if len(result_dict) >= self._segments_needed:
                self._log.debug(
                    "%s %s len(result_dict) = %s: enough" % (
                    self._collection_id,
                    self._key,
                    len(result_dict),
                ))
                self._pending.kill()
                break

        # if anything is still running, get rid of it
        self._pending.join(timeout, raise_error=True)

        if len(result_dict) < self._segments_needed:
            error_message = "(%s) %s too few valid results %s" % (
                self._collection_id,
                self._key,
                len(result_dict),
            )
            self._log.error(error_message)
            raise RetrieveFailedError(error_message)

        if all(completed_list):
            self._log.debug("(%s) %s all nodes say completed" % (
                self._collection_id,
                self._key,
            ))
            return result_dict, True

        if any(completed_list):
            error_message = "(%s) %s inconsistent completed %s" % (
                self._collection_id,
                self._key,
                completed_list,
            )
            self._log.error(error_message)
            raise RetrieveFailedError(error_message)
            
        self._log.debug("(%s) %s all nodes say NOT completed" % (
            self._collection_id,
            self._key,
        ))
        return result_dict, False
        
    def _process_finished_task(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            self._log.debug(
                "(%s) %s %s task ends with GreenletExit" % (
                    self._collection_id,
                    self._key,
                    task.data_reader.node_name,
                )
            )
            return None

        if not task.successful():
            # 2011-10-07 dougfort -- I don't know how a task
            # could be unsuccessful
            self._log.warn("(%s) %s %s task unsuccessful" % (
                self._collection_id,
                self._key,
                task.data_reader.node_name,
            ))
            return None

        self._log.debug("(%s) %s %s task successful" % (
            self._collection_id,
            self._key,
            task.data_reader.node_name,
        ))

        # we expect retrieve_key_start to return the tuple
        # (<data-segment>, <zfec-padding-size>, <completion-status>, )
        # where completion-status is a boolean
        # if the retrieve failed in some way, retrieve_key_start
        # returns None

        if task.value is None:
            self._log.debug(
                "(%s) %s %s task value is None" % (
                    self._collection_id,
                    self._key,
                    task.data_reader.node_name,
                )
            )
            return None

        data_segment, zfec_padding_size, completion_status = task.value

        self._log.debug("(%s) %s %s task successful complete = %r" % (
            self._collection_id,
            self._key,
            task.data_reader.node_name, 
            completion_status,
        ))

        return data_segment, zfec_padding_size, completion_status

