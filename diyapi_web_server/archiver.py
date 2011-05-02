# -*- coding: utf-8 -*-
"""
archiver.py

A class that sends data segments to data writers.
"""
import sys
import logging
import hashlib
import zlib
from collections import defaultdict

import gevent
from gevent.pool import GreenletSet

from diyapi_web_server.exceptions import (
    AlreadyInProgress,
    ArchiveFailedError,
)


HANDOFF_NUM = 2


class Archiver(object):
    """Sends data segments to data writers."""
    def __init__(self, data_writers, avatar_id, key, timestamp):
        self.log = logging.getLogger(
            'Archiver(avatar_id=%d, key=%r)' % (avatar_id, key))
        self.data_writers = data_writers
        self.avatar_id = avatar_id
        self.key = key
        self.version_number = 0
        self.timestamp = timestamp
        self.sequence_number = 0
        self._adler32s = {}
        self._md5s = defaultdict(hashlib.md5)
        self._handoff_writers = defaultdict(list)
        self._pending = GreenletSet()
        self._done = []

    def _join(self, timeout, handoff=False):
        self._pending.join(timeout, True)
        # make sure _done_link gets run first by cooperating
        gevent.sleep(0)
        if not self._pending:
            return
        if handoff or self.sequence_number != 0:
            raise ArchiveFailedError()
        for task in self._pending.greenlets.copy():
            if len(self._done) < HANDOFF_NUM:
                raise ArchiveFailedError()
            for done_task in self._done[:HANDOFF_NUM]:
                self._handoff_writers[task.segment_number].append(
                    done_task.data_writer)
                self._spawn(
                    task.segment_number,
                    done_task.data_writer,
                    getattr(done_task.data_writer, task.method_name),
                    *task.args
                )
            del self._done[:HANDOFF_NUM]
            task.kill()
        self._join(timeout, True)

    def _done_link(self, task):
        if isinstance(task.value, gevent.GreenletExit):
            return
        self._done.append(task)

    def _spawn(self, segment_number, data_writer, run, *args):
        method_name = run.__name__
        task = self._pending.spawn(run, *args)
        task.rawlink(self._done_link)
        task.segment_number = segment_number
        task.data_writer = data_writer
        task.method_name = method_name
        return task

    def _do_handoff(self, timeout):
        if not self._handoff_writers:
            return
        for segment_number in self._handoff_writers:
            for data_writer in self._handoff_writers[segment_number]:
                self._spawn(
                    segment_number,
                    data_writer,
                    data_writer.hinted_handoff,
                    self.avatar_id,
                    self.timestamp,
                    self.key,
                    self.version_number,
                    segment_number,
                    self.data_writers[segment_number - 1].exchange
                )
        self._join(timeout)

    def archive_slice(self, segments, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_number = i + 1
            self._adler32s[segment_number] = zlib.adler32(
                segment,
                self._adler32s.get(segment_number, 1)
            )
            self._md5s[segment_number].update(segment)
            if segment_number in self._handoff_writers:
                data_writers = self._handoff_writers[segment_number]
            else:
                data_writers = [self.data_writers[i]]
            if self.sequence_number == 0:
                for data_writer in data_writers:
                    self._spawn(
                        segment_number,
                        data_writer,
                        data_writer.archive_key_start,
                        self.avatar_id,
                        self.timestamp,
                        self.sequence_number,
                        self.key,
                        self.version_number,
                        segment_number,
                        segment
                    )
            else:
                for data_writer in data_writers:
                    self._spawn(
                        segment_number,
                        data_writer,
                        data_writer.archive_key_next,
                        self.sequence_number,
                        segment
                    )
        self._join(timeout)
        self._done = []
        self.sequence_number += 1

    def archive_final(self, file_size, file_adler32, file_md5,
                      segments, timeout=None):
        if self._pending:
            raise AlreadyInProgress()
        for i, segment in enumerate(segments):
            segment_number = i + 1
            self._adler32s[segment_number] = zlib.adler32(
                segment,
                self._adler32s.get(segment_number, 1)
            )
            self._md5s[segment_number].update(segment)
            if segment_number in self._handoff_writers:
                data_writers = self._handoff_writers[segment_number]
            else:
                data_writers = [self.data_writers[i]]
            if self.sequence_number == 0:
                for data_writer in data_writers:
                    self._spawn(
                        segment_number,
                        data_writer,
                        data_writer.archive_key_entire,
                        self.avatar_id,
                        self.timestamp,
                        self.key,
                        self.version_number,
                        segment_number,
                        file_size,
                        file_adler32,
                        file_md5,
                        self._adler32s[segment_number],
                        self._md5s[segment_number].digest(),
                        segment
                    )
            else:
                for data_writer in data_writers:
                    self._spawn(
                        segment_number,
                        data_writer,
                        data_writer.archive_key_final,
                        self.sequence_number,
                        file_size,
                        file_adler32,
                        file_md5,
                        self._adler32s[segment_number],
                        self._md5s[segment_number].digest(),
                        segment
                    )
        self._join(timeout)
        self._do_handoff(timeout)
        result = sum([task.value for task in self._done])
        self._done = []
        return result
