# -*- coding: utf-8 -*-
"""
archiver.py

A class that sends data segments to data writers.
"""
import logging
import os

import gevent
import gevent.pool

from web_server.exceptions import (
    AlreadyInProgress,
    ArchiveFailedError,
)

_local_node_name = os.environ["NIMBUSIO_NODE_NAME"]

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
        conjoined_part
    ):
        self.log = logging.getLogger(
            'Archiver(collection_id=%d, key=%r)' % (collection_id, key))
        self.data_writers = data_writers
        self.collection_id = collection_id
        self.key = key
        self._unified_id = unified_id
        self.timestamp = timestamp
        self._meta_dict = meta_dict
        self._conjoined_part = conjoined_part
        self.sequence_num = 0
        self._pending = gevent.pool.Group()
        self._done = []

    def _join(self, timeout):
        self._pending.join(timeout, True)
        # make sure _done_link gets run first by cooperating
        gevent.sleep(0)
        if not self._pending:
            return
        raise ArchiveFailedError("%s tasks incomplete" % (len(self._pending),))

    def _done_link(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            return
        self._done.append(task)

    def _spawn(self, segment_num, data_writer, run, *args):
        method_name = run.__name__
        task = self._pending.spawn(run, *args)
        task.rawlink(self._done_link)
        task.segment_num = segment_num
        task.data_writer = data_writer
        task.method_name = method_name
        return task

    def archive_slice(self, segments, zfec_padding_size, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_num = i + 1
            data_writers = [self.data_writers[i]]
            if self.sequence_num == 0:
                for data_writer in data_writers:
                    self._spawn(
                        segment_num,
                        data_writer,
                        data_writer.archive_key_start,
                        self.collection_id,
                        self.key,
                        self._unified_id,
                        self.timestamp,
                        self._conjoined_part,
                        segment_num,
                        zfec_padding_size,
                        self.sequence_num,
                        segment,
                        _local_node_name
                    )
            else:
                for data_writer in data_writers:
                    self._spawn(
                        segment_num,
                        data_writer,
                        data_writer.archive_key_next,
                        self.collection_id,
                        self.key,
                        self._unified_id,
                        self.timestamp,
                        self._conjoined_part,
                        segment_num,
                        zfec_padding_size,
                        self.sequence_num,
                        segment,
                        _local_node_name
                    )
        self._join(timeout)
        self._done = []
        self.sequence_num += 1

    def archive_final(
        self, 
        file_size, 
        file_adler32, 
        file_md5,
        segments, 
        zfec_padding_size,
        timeout=None
    ):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_num = i + 1
            data_writer = self.data_writers[i]
            if self.sequence_num == 0:
                self._spawn(
                    segment_num,
                    data_writer,
                    data_writer.archive_key_entire,
                    self.collection_id,
                    self.key,
                    self._unified_id,
                    self.timestamp,
                    self._conjoined_part,
                    self._meta_dict,
                    segment_num,
                    zfec_padding_size,
                    file_size,
                    file_adler32,
                    file_md5,
                    segment,
                    _local_node_name
                )
            else:
                self._spawn(
                    segment_num,
                    data_writer,
                    data_writer.archive_key_final,
                    self.collection_id,
                    self.key,
                    self._unified_id,
                    self.timestamp,
                    self._conjoined_part,
                    self._meta_dict,
                    segment_num,
                    zfec_padding_size,
                    self.sequence_num,
                    file_size,
                    file_adler32,
                    file_md5,
                    segment,
                    _local_node_name
                )
        self._join(timeout)
        self._done = []

