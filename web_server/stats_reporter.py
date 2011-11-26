# -*- coding: utf-8 -*-
"""
A Greenlet to periodicaly report statistics of the web server
"""
import logging

from  gevent.greenlet import Greenlet
from  gevent.event import Event

_interval = 60.0

class StatsReporter(Greenlet):
    """
    A Greenlet to periodicaly report statistics of the web server
    """
    def __init__(self, stats, event_push_client):
        Greenlet.__init__(self)
        self._log = logging.getLogger(str(self))
        self._stats = stats
        self._event_push_client = event_push_client
        self._halt_event = Event()

    def _run(self):
        self._log.debug("starting")

        while not self._halt_event.is_set():
            self._log.info(
                "archives: %(archives)s; retrieves: %(retrieves)s" \
                % self._stats
            )
            self._event_push_client.info(
                "web-server-active-archives",
                "number of archive requests currently running",
                count=self._stats["archives"]
            )
            self._event_push_client.info(
                "web-server-active-retrieves",
                "number of retrieve requests currently running",
                count=self._stats["retrieves"]
            )
            self._halt_event.wait(_interval)

        self._log.debug("ending")

    def join(self, timeout=None):
        self._log("join")
        self._halt_event.set()
        Greenlet.join(self, timeout)

    def __str__(self):
        return "StatsReporter"

