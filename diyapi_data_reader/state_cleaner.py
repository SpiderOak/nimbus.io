# -*- coding: utf-8 -*-
"""
state_cleaner.py

A time queue action to perioidcially clean out the state
"""
import logging
import time

_polling_interval = 10.0

class StateCleaner(object):
    """A time queue action to periodically clean out the state"""
    def __init__(self, state):
        self._log = logging.getLogger("StateCleaner")
        self._state = state

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        if halt_event.is_set():
            return
                
        current_time = time.time()
        overdue_requests = list()
        for state_key, state_entry in self._state["active-requests"].items():
            if halt_event.is_set():
                return
            if current_time > state_entry.timeout:
                overdue_requests.append(state_key)

        for state_key in overdue_requests:        
            if halt_event.is_set():
                return

            state_entry = self._state["active-requests"].pop(state_key)
            self._log.warn("%s timed out waiting reply %s" % ( 
                state_key, state_entry
            ))

        return [(self.run, self.next_run(), )]

