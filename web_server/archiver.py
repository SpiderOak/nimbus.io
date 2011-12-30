# -*- coding: utf-8 -*-
"""
archiver.py

A class that sends data segments to data writers.
"""
import logging
import hashlib
import zlib
from collections import defaultdict

import gevent
import gevent.pool

from web_server.exceptions import (
    AlreadyInProgress,
    ArchiveFailedError,
)

class Archiver(object):
    """Sends data segments to data writers."""
    def __init__(
        self, 
        data_writers, 
        collection_id, 
        key, 
        timestamp, 
        meta_dict, 
        conjoined_dict
    ):
        self.log = logging.getLogger(
            'Archiver(collection_id=%d, key=%r)' % (collection_id, key))
        self.data_writers = data_writers
        self.collection_id = collection_id
        self.key = key
        self.timestamp = timestamp
        self._meta_dict = meta_dict
        if conjoined_dict["conjoined_identifier"] is None:
            self._conjoined_identifier_hex = None
        else:
            self._conjoined_identifier_hex = \
                    conjoined_dict["conjoined_identifier"].hex
        self._conjoined_part = conjoined_dict["conjoined_part"]
        self.sequence_num = 0
        self._adler32s = {}
        self._md5s = defaultdict(hashlib.md5)
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

    def archive_slice(self, segments, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_num = i + 1
            self._adler32s[segment_num] = zlib.adler32(
                segment,
                self._adler32s.get(segment_num, 1)
            )
            self._md5s[segment_num].update(segment)
            data_writers = [self.data_writers[i]]
            if self.sequence_num == 0:
                for data_writer in data_writers:
                    self._spawn(
                        segment_num,
                        data_writer,
                        data_writer.archive_key_start,
                        self.collection_id,
                        self.key,
                        self.timestamp,
                        segment_num,
                        self.sequence_num,
                        segment
                    )
            else:
                for data_writer in data_writers:
                    self._spawn(
                        segment_num,
                        data_writer,
                        data_writer.archive_key_next,
                        self.collection_id,
                        self.key,
                        self.timestamp,
                        segment_num,
                        self.sequence_num,
                        segment
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
        timeout=None
    ):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_num = i + 1
            self._adler32s[segment_num] = zlib.adler32(
                segment,
                self._adler32s.get(segment_num, 1)
            )
            self._md5s[segment_num].update(segment)
            data_writer = self.data_writers[i]
            if self.sequence_num == 0:
                self._spawn(
                    segment_num,
                    data_writer,
                    data_writer.archive_key_entire,
                    self.collection_id,
                    self.key,
                    self.timestamp,
                    self._meta_dict,
                    self._conjoined_identifier_hex,
                    self._conjoined_part,
                    segment_num,
                    file_size,
                    file_adler32,
                    file_md5,
                    segment
                )
            else:
                self._spawn(
                    segment_num,
                    data_writer,
                    data_writer.archive_key_final,
                    self.collection_id,
                    self.key,
                    self.timestamp,
                    self._meta_dict,
                    self._conjoined_identifier_hex,
                    self._conjoined_part,
                    segment_num,
                    self.sequence_num,
                    file_size,
                    file_adler32,
                    file_md5,
                    segment
                )
        self._join(timeout)
        self._done = []

