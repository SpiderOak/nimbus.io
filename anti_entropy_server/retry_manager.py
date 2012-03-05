# -*- coding: utf-8 -*-
"""
retry_manager.py

A time queue action to perioidcially retry consistency checks
"""
import logging
import time

_polling_interval = 15.0 * 60.0

class RetryManager(object):
    """A time queue action to periodically retry consistency checks"""
    def __init__(self, state, start_consistency_check):
        self._log = logging.getLogger("StateCleaner")
        self._state = state
        self._start_consistency_check = start_consistency_check

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        if halt_event.is_set():
            self._log.info("halt-event is set: exiting")
            return
                
        current_time = time.time()
        next_retry_list = list()
        for retry_entry in self._state["retry-list"]:
            if current_time >= retry_entry.retry_time:
                self._start_consistency_check(
                    self._state,
                    retry_entry.collection_id, 
                    row_id=retry_entry.row_id,
                    retry_count=retry_entry.retry_count +1
                )
            else:
                next_retry_list.append(retry_entry)
        self._state["retry-list"] = next_retry_list

        return [(self.run, self.next_run(), )]

