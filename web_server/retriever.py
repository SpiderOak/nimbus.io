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

from tools.data_definitions import segment_status_final

from web_server.exceptions import RetrieveFailedError
from web_server.local_database_util import current_status_of_key

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
        segments_needed
    ):
        self._log = logging.getLogger("Retriever")
        self._log.info('collection_id=%d, key=%r' % (collection_id, key, ))
        self._node_local_connection = node_local_connection
        self._data_readers = data_readers
        self._collection_id = collection_id
        self._key = key
        self._version_id = version_id
        self._segments_needed = segments_needed
        self._pending = gevent.pool.Group()
        self._finished_tasks = gevent.queue.Queue()
        self._sequence = 0

    def _done_link(self, task):
        if task.sequence != self._sequence:
            self._log.debug("_done_link ignore task %s seq %s expect %s" % (
                task.data_reader.node_name,
                task.sequence,
                self._sequence
            ))
        else:
            self._finished_tasks.put(task, block=True)

    def retrieve(self, timeout):
        # TODO: find a non-blocking way to do this
        # TODO: don't just use the local node, it might be wrong
        conjoined_row, segment_rows = current_status_of_key(
            self._node_local_connection,
            self._collection_id, 
            self._key,
            self._version_id
        )

        if len(segment_rows) == 0:
            raise RetrieveFailedError("key not found %s %s" % (
                self._collection_id, self._key,
            ))

        is_available = False
        if conjoined_row is None:
            is_available = segment_rows[0].status == segment_status_final
        else:
            is_available = conjoined_row.delete_timestamp is None

        if not is_available:
            raise RetrieveFailedError("key is not available %s %s" % (
                self._collection_id, self._key,
            ))

        for segment_row in segment_rows:
            # spawn retrieve_key start, then spawn retrieve key next
            # until we are done
            start = True
            while True:
                self._sequence += 1
                self._log.debug("retrieve: starting sequence %s part %s" % (
                    self._sequence, segment_row.conjoined_part,
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
                        function = data_reader.retrieve_key_start
                    else:
                        function = data_reader.retrieve_key_next
                    task = self._pending.spawn(
                        function, 
                        segment_row.unified_id,
                        segment_number
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

                if start:
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

