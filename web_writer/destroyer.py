# -*- coding: utf-8 -*-
"""
destroyer.py

A class that performs a destroy query on all data writers.
"""
import logging
import os
import time

import gevent
import gevent.pool
import gevent.queue

from web_writer.exceptions import DestroyFailedError

from web_server.local_database_util import current_status_of_key, \
        current_status_of_version

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]
_task_timeout = 60.0

class Destroyer(object):
    """Performs a destroy query on all data writers."""
    def __init__(
        self, 
        node_local_connection,
        data_writers,
        collection_id, 
        key,
        unified_id_to_delete,
        unified_id,
        timestamp        
    ):
        self._log = logging.getLogger('Destroyer')
        self._log.info('collection_id=%d, key=%r' % (collection_id, key, ))
        self._node_local_connection = node_local_connection
        self._data_writers = data_writers
        self._collection_id = collection_id
        self._key = key
        self._unified_id_to_delete = unified_id_to_delete
        self._unified_id = unified_id
        self.timestamp = timestamp
        self._pending = gevent.pool.Group()
        self._finished_tasks = gevent.queue.Queue()

    def _done_link(self, task):
        self._finished_tasks.put(task, block=True)

    def destroy(self, timeout=None):
        # TODO: find a non-blocking way to do this
        if self._unified_id_to_delete is None:
            status_rows = current_status_of_key(
                self._node_local_connection, 
                self._collection_id,
                self._key
            )
        else:
            status_rows = current_status_of_version(
                self._node_local_connection, 
                self._unified_id_to_delete
            )

        if len(status_rows) == 0:
            raise DestroyFailedError("no status rows found")

        file_size = sum([row.seg_file_size for row in status_rows])

        for i, data_writer in enumerate(self._data_writers):
            segment_num = i + 1
            task = self._pending.spawn(
                data_writer.destroy_key,
                self._collection_id,
                self._key,
                self._unified_id_to_delete,
                self._unified_id,
                self.timestamp,
                segment_num,
                _local_node_name,
            )
            task.link(self._done_link)
            task.node_name = data_writer.node_name

        self._process_node_replies(timeout)

        return file_size

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
                    self._log.error(error_message)
                    raise DestroyFailedError(error_message)

                self._log.warn("timeout waiting for completed task")
                continue

            finished_count += 1
            if isinstance(task.value, gevent.GreenletExit):
                self._log.debug(
                    "(%s) %s %s %s task ends with GreenletExit" % (
                        self._collection_id,
                        self._key,
                        self._unified_id,
                        task.node_name,
                    )
                )
                error_count += 1
                continue

            if not task.successful():
                # 2011-10-07 dougfort -- I don't know how a task
                # could be unsuccessful
                self._log.error("(%s) %s %s %s task unsuccessful" % (
                    self._collection_id,
                    self._key,
                    self._unified_id,
                    task.node_name,
                ))
                error_count += 1
                continue

            if task.value["result"] != "success":
                self._log.error(
                    "(%s) %s %s %s task ends with %s" % (
                        self._collection_id,
                        self._key,
                        self._unified_id,
                        task.node_name,
                        task.value["error-message"]
                    )
                )
                error_count += 1
                continue

            self._log.debug("(%s) %s %s %s task successful" % (
                self._collection_id,
                self._key,
                self._unified_id,
                task.node_name,
            ))

        if error_count > 0:
            error_message = \
                "%s errors %s %s %s" % (
                    error_count,
                    self._collection_id,
                    self._key,
                    self._unified_id
                )
            self._log.error(error_message)
            raise DestroyFailedError(error_message)

