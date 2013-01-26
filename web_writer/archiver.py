# -*- coding: utf-8 -*-
"""
archiver.py

A class that sends data segments to data writers.
"""
import logging
import os
import time

import gevent
import gevent.pool
import gevent.queue

from web_writer.exceptions import ArchiveFailedError

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_task_timeout = 60.0

class Archiver(object):
    """Sends data segments to data writers."""
    def __init__(
        self, 
        data_writers, 
        collection_id, 
        key, 
        unified_id,
        timestamp, 
        meta_dict, 
        conjoined_part,
        user_request_id
    ):
        self._log = logging.getLogger(
            'Archiver(collection_id=%d, key=%r)' % (collection_id, key))
        self._data_writers = data_writers
        self._collection_id = collection_id
        self._key = key
        self._unified_id = unified_id
        self._timestamp = timestamp
        self._meta_dict = meta_dict
        self._conjoined_part = conjoined_part
        self._user_request_id = user_request_id
        self._sequence_num = 0
        self._pending = gevent.pool.Group()
        self._finished_tasks = gevent.queue.Queue()

    def _done_link(self, task):
        self._finished_tasks.put(task, block=True)

    def _unhandled_greenlet_exception(self, greenlet_object):
        self._log.error("request {0}: " \
                        "unhandled greenlet exception {1} {2} {3}".format(
                        self._user_request_id,
                        str(greenlet_object),
                        greenlet_object.exception.__class__.__name__,
                        str(greenlet_object.exception)))
 
    def archive_slice(self, segments, zfec_padding_size, timeout=None):
        for i, segment in enumerate(segments):
            segment_num = i + 1
            data_writer = self._data_writers[i]
            if self._sequence_num == 0:
                task = self._pending.spawn(
                    data_writer.archive_key_start,
                    self._collection_id,
                    self._key,
                    self._unified_id,
                    self._timestamp,
                    self._conjoined_part,
                    segment_num,
                    zfec_padding_size,
                    self._sequence_num,
                    segment,
                    _local_node_name,
                    self._user_request_id
                )
            else:
                task = self._pending.spawn(
                    data_writer.archive_key_next,
                    self._collection_id,
                    self._key,
                    self._unified_id,
                    self._timestamp,
                    self._conjoined_part,
                    segment_num,
                    zfec_padding_size,
                    self._sequence_num,
                    segment,
                    _local_node_name,
                    self._user_request_id
                )
            task.node_name = data_writer.node_name
            task.link(self._done_link)
            task.link_exception(self._unhandled_greenlet_exception)

        self._process_node_replies(timeout)
        self._sequence_num += 1

    def archive_final(
        self, 
        file_size, 
        file_adler32, 
        file_md5,
        segments, 
        zfec_padding_size,
        timeout=None
    ):
        for i, segment in enumerate(segments):
            segment_num = i + 1
            data_writer = self._data_writers[i]
            if self._sequence_num == 0:
                task = self._pending.spawn(
                    data_writer.archive_key_entire,
                    self._collection_id,
                    self._key,
                    self._unified_id,
                    self._timestamp,
                    self._conjoined_part,
                    self._meta_dict,
                    segment_num,
                    zfec_padding_size,
                    file_size,
                    file_adler32,
                    file_md5,
                    segment,
                    _local_node_name,
                    self._user_request_id
                )
            else:
                task = self._pending.spawn(
                    data_writer.archive_key_final,
                    self._collection_id,
                    self._key,
                    self._unified_id,
                    self._timestamp,
                    self._conjoined_part,
                    self._meta_dict,
                    segment_num,
                    zfec_padding_size,
                    self._sequence_num,
                    file_size,
                    file_adler32,
                    file_md5,
                    segment,
                    _local_node_name,
                    self._user_request_id
                )
            task.node_name = data_writer.node_name
            task.link(self._done_link)
            task.link_exception(self._unhandled_greenlet_exception)

        self._process_node_replies(timeout)

    def _process_node_replies(self, timeout):
        finished_count = 0
        error_count = 0
        start_time = time.time()

        # block on the finished_tasks queue until done
        while finished_count < len(self._data_writers):
            try:
                task = self._finished_tasks.get(block=True, 
                                                timeout=_task_timeout)
            except gevent.queue.Empty:
                elapsed_time = time.time() - start_time
                if elapsed_time > timeout:
                    error_message = \
                        "timed out _finished_tasks %s %s %s" % (
                            self._collection_id,
                            self._key,
                            self._unified_id
                        )
                    self._log.error("request {0}: {1}".format(
                                    self._user_request_id,
                                    error_message))
                    raise ArchiveFailedError(error_message)

                self._log.warn("request {0}: " \
                               "timeout waiting for completed task".format(
                                self._user_request_id,
                                error_message))
                continue

            finished_count += 1
            if isinstance(task.value, gevent.GreenletExit):
                self._log.debug("request {0}: " \
                                "({1}) {2} {3} {4} " \
                                "task ends with GreenletExit".format(
                                self._user_request_id,
                                self._collection_id,
                                self._key,
                                self._unified_id,
                                task.node_name))
                error_count += 1
                continue

            if not task.successful():
                # 2011-10-07 dougfort -- I don't know how a task
                # could be unsuccessful
                self._log.error("request {0}: " \
                                "({1}) {2} {3} {4} task unsuccessful".format(
                                self._user_request_id,
                                self._collection_id,
                                self._key,
                                self._unified_id,
                                task.node_name))
                error_count += 1
                continue

            if task.value["result"] != "success":
                self._log.error("request {0}: " \
                                "({1}) {2} {3} {4} task ends with {5}".format(
                                self._user_request_id,
                                self._collection_id,
                                self._key,
                                self._unified_id,
                                task.node_name,
                                task.value["error-message"]))
                error_count += 1
                continue

            self._log.debug("request {0}: " \
                            " ({1}) {2} {3} {4} task successful".format(
                            self._user_request_id,
                            self._collection_id,
                            self._key,
                            self._unified_id,
                            task.node_name))

        if error_count > 0:
            error_message = \
                "%s errors %s %s %s" % (
                    error_count,
                    self._collection_id,
                    self._key,
                    self._unified_id
                )
            self._log.error("request {0}: {1}".format(self._user_request_id,
                                                      error_message))
            raise ArchiveFailedError(error_message)
