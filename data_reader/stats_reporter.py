# -*- coding: utf-8 -*-
"""
stats_reporter.py

A time queue action to perioidically report statistics
"""
import logging
import time

_reporting_interval = 60.0

class StatsReporter(object):
    """A time queue action to periodically report statistics"""
    def __init__(self, state):
        self._log = logging.getLogger("StatsReporter")
        self._state = state

        self._log.info(
            "created with reporting interval %s" % (_reporting_interval)
        )

    @classmethod
    def next_run(cls):
        return time.time() + _reporting_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return

        self._state["event-push-client"].info(
            "data-reader-receive-queue-size", 
            "queue size",
            queue_size=len(self._state["receive-queue"]),
        )  
        self._log.info("receive-qeue size = %s" % (
            len(self._state["receive-queue"]),
        ))

        return [(self.run, self.next_run(), )]

