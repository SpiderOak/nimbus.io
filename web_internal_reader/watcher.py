# -*- coding: utf-8 -*-
"""
A Greenlet to watch the web_server internals
"""
import logging

from  gevent.greenlet import Greenlet
from  gevent.event import Event

_interval = 60.0

class Watcher(Greenlet):
    """
    A Greenlet to watch web server internals
    """
    def __init__(self, stats, reader_clients, event_push_client):
        Greenlet.__init__(self)
        self._log = logging.getLogger(str(self))
        self._stats = stats
        self._reader_clients = reader_clients
        self._event_push_client = event_push_client
        self._halt_event = Event()

    def _run(self):
        self._log.debug("starting")

        while not self._halt_event.is_set():

            reader_info = list()
            for client in self._reader_clients:
                reader_info.append(client.queue_size)

            self._log.info("retrieves: %(retrieves)s" % self._stats)
            self._event_push_client.info(
                "web-server-stats",
                "web server stats",
                stats=self._stats,
                reader=reader_info
            )
            self._halt_event.wait(_interval)

        self._log.debug("ending")

    def join(self, timeout=None):
        self._log.debug("joining")
        self._halt_event.set()
        Greenlet.join(self, timeout)
        self._log.debug("join complete")

    def __str__(self):
        return "Watcher"

