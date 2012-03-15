# -*- coding: utf-8 -*-
"""
sync_manager.py

A time queue action to perioidically sync the output value file
"""
import logging
import time

_sync_interval = 1.0

class SyncManager(object):
    """A time queue action to periodically sync the output value file"""
    def __init__(self, writer):
        self._log = logging.getLogger("SyncManager")
        self._writer = writer

        self._log.info(
            "created with sync interval %s" % (_sync_interval)
        )

    @classmethod
    def next_run(cls):
        return time.time() + _sync_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return

        self._writer.sync_value_file()


        return [(self.run, self.next_run(), )]

