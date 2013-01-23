# -*- coding: utf-8 -*-
"""
retriever.py

A class that retrieves data from data readers.
"""
import logging
import time
import uuid

import gevent
import gevent.pool
import gevent.queue

from web_internal_reader.exceptions import RetrieveFailedError

# 2012-06-13 dougfort - we don't want to block too long here
# because if a node is down, we will block a lot
_task_timeout = 1.0

class Retriever(object):
    """Retrieves data from data readers."""
    def __init__(
        self, 
        node_local_connection,
        data_readers, 
        collection_id, 
        key, 
        unified_id,
        conjoined_part,
        block_offset,
        block_count,
        segments_needed,
        user_request_id
    ):
        self._log = logging.getLogger("Retriever")
        self._log.info("request {0} {1}, {2}, {3}, {4}, {5} {6}".format(
            user_request_id,
            collection_id, 
            key, 
            unified_id,
            conjoined_part,
            block_offset,
            block_count,
        ))
        self._node_local_connection = node_local_connection
        self._data_readers = data_readers
        self._collection_id = collection_id
        self._key = key
        self._unified_id = unified_id
        self._conjoined_part = conjoined_part
        self._block_offset = block_offset
        self._block_count = block_count
        self._segments_needed = segments_needed
        self._user_request_id = user_request_id
        self._pending = gevent.pool.Group()
        self._finished_tasks = gevent.queue.Queue()
        self._sequence = 0

    def _unhandled_greenlet_exception(self, greenlet_object):
        self._log.error("request {0}: " \
                        "unhandled greenlet exception {1} {2} {3}".format(
                        self._user_request_id,
                        str(greenlet_object),
                        greenlet_object.exception.__class__.__name__,
                        str(greenlet_object.exception)))
                
    def _done_link(self, task):
        if task.sequence != self._sequence:
            self._log.debug("request {0} _done_link ignore task {1} seq {2} expect {3}".format(
                self._user_request_id,
                task.data_reader.node_name,
                task.sequence,
                self._sequence
            ))
        else:
            self._finished_tasks.put(task, block=True)

    def retrieve(self, timeout):
        retrieve_id = uuid.uuid1().hex

        # spawn retrieve_key start, then spawn retrieve key next
        # until we are done
        start = True
        while True:
            self._sequence += 1
            self._log.debug("request {0} retrieve: {1} {2} {3} {4}".format(
                self._user_request_id,
                self._sequence, 
                self._unified_id, 
                self._conjoined_part,
                retrieve_id
            ))
            # send a request to all node
            for i, data_reader in enumerate(self._data_readers):
                if not data_reader.connected:
                    self._log.warn("request {0} ignoring disconnected reader {1}".format(
                        self._user_request_id, str(data_reader),
                    ))
                    continue

                segment_number = i + 1
                if start:
                    task = self._pending.spawn(
                        data_reader.retrieve_key_start,
                        retrieve_id,
                        self._sequence,
                        self._collection_id,
                        self._key,
                        self._unified_id,
                        self._conjoined_part,
                        segment_number,
                        self._block_offset,
                        self._block_count,
                        self._user_request_id,
                    )
                else:
                    task = self._pending.spawn(
                        data_reader.retrieve_key_next,
                        retrieve_id,
                        self._sequence,
                        self._collection_id,
                        self._key,
                        self._unified_id,
                        self._conjoined_part,
                        segment_number,
                        self._block_offset,
                        self._block_count,
                        self._user_request_id
                    )
                task.link(self._done_link)
                task.link_exception(self._unhandled_greenlet_exception)
                task.segment_number = segment_number
                task.data_reader = data_reader
                task.sequence = self._sequence

            # wait for, and process, replies from the nodes
            result_dict, completed = self._process_node_replies(timeout)
            self._log.debug("request {0} retrieve: completed sequence {1}".format(
                self._user_request_id, self._sequence,
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
                task = self._finished_tasks.get(block=True, 
                                                timeout=_task_timeout)
            except gevent.queue.Empty:
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout:
                    error_message = \
                        "timed out _finished_tasks %s %s" % (
                            self._collection_id,
                            self._key,
                        )
                    self._log.error("request {0} {1}".format(self._user_request_id, 
                                                            error_message))
                    raise RetrieveFailedError(error_message)

                continue

            # if we previously only waited for 8/10 replies, we may still get
            # those other 2 replies coming in even though we have moved on.
            if task.sequence != self._sequence:
                self._log.debug(
                    "request {0} _process_node_replies ignore task {1} seq {2} expect {3}".format(
                        self._user_request_id,
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
                    "request {0} {1} {2} len(result_dict) = {3}: enough".format(
                    self._user_request_id,
                    self._collection_id,
                    self._key,
                    len(result_dict),
                ))
                self._pending.kill()
                break

        # if anything is still running, get rid of it
        self._pending.join(timeout, raise_error=True)

        if len(result_dict) < self._segments_needed:
            error_message = "({0}) {1} too few valid results {2}".format(
                self._collection_id,
                self._key,
                len(result_dict)
            )
            self._log.error("request {0} {1}".format(self._user_request_id, 
                                                     error_message))
            raise RetrieveFailedError(error_message)

        if all(completed_list):
            self._log.debug("request {0} ({1}) {2} all nodes say completed".format(
                self._user_request_id,
                self._collection_id,
                self._key,
            ))
            return result_dict, True

        if any(completed_list):
            error_message = "({0}) {1} inconsistent completed {2}".format(
                self._collection_id,
                self._key,
                completed_list
            )
            self._log.error("request {0} {1}".format(self._user_request_id, 
                                                     error_message))
            raise RetrieveFailedError(error_message)
            
        self._log.debug("request {0} ({1}) {2} all nodes say NOT completed".format(
            self._user_request_id,
            self._collection_id,
            self._key,
        ))
        return result_dict, False
        
    def _process_finished_task(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            self._log.debug(
                "request {0} ({1}) {2} {3} task ends with GreenletExit".format(
                    self._user_request_id,
                    self._collection_id,
                    self._key,
                    task.data_reader.node_name,
                )
            )
            return None

        if not task.successful():
            # 2011-10-07 dougfort -- I don't know how a task
            # could be unsuccessful
            self._log.warn("request {0} ({1}) {2} {3} task unsuccessful".format(
                self._user_request_id,
                self._collection_id,
                self._key,
                task.data_reader.node_name,
            ))
            return None

        self._log.debug("request {0} ({1}) {2} {3} task successful".format(
            self._user_request_id,
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
                "request {0} ({1}) {2} {3} task value is None".format(
                    self._user_request_id,
                    self._collection_id,
                    self._key,
                    task.data_reader.node_name,
                )
            )
            return None

        data_segment, zfec_padding_size, completion_status = task.value

        self._log.debug("request {0} ({1}) {2} {3} task successful complete = {4}".format(
            self._user_request_id,
            self._collection_id,
            self._key,
            task.data_reader.node_name, 
            completion_status,
        ))

        return data_segment, zfec_padding_size, completion_status
