# -*- coding: utf-8 -*-
"""
heartbeater.py

A time queue action to perioidcially send a heartbeat
"""
import logging
import time

class Heartbeater(object):
    """A time queue action to periodically send a heartbeat"""
    def __init__(self, state, heartbeat_interval):
        self._log = logging.getLogger("Heartbeater")
        self._state = state
        self._heartbeat_interval = heartbeat_interval

    def next_run(self):
        return time.time() + self._heartbeat_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return

        # send some form of heartbeat here, probably PUB/SUB

        return [(self.run, self.next_run(), )]

