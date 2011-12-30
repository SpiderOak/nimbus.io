# -*- coding: utf-8 -*-
"""
state_cleaner.py

A time queue action to periodically clean out the state
"""
import datetime
import logging
import time

from space_accounting_server.space_accounting_database import \
        SpaceAccountingDatabase
from space_accounting_server.util import floor_hour

class StateCleaner(object):
    """A time queue action to periodically clean out the state"""
    def __init__(self, state):
        self._log = logging.getLogger("StateCleaner")
        self._state = state

    @classmethod
    def next_run(cls):
        log = logging.getLogger("StateCleaner")
        current_time = datetime.datetime.now()
        next_time = datetime.datetime(
            year = current_time.year,
            month = current_time.month,
            day = current_time.day,
            hour = current_time.hour,
            minute = 5,
            second = 0,
            microsecond = 0
        )
        if current_time.minute >= 5:
            next_time += datetime.timedelta(hours=1)
        log.info("next dump time = %s", (next_time, ))
        return time.mktime(next_time.timetuple())

    def run(self, halt_event):
        if halt_event.is_set():
            return
                
        # we want to dump everything for the previous hour
        current_time = datetime.datetime.now()
        current_hour = floor_hour(current_time)
        prev_hour = current_hour -  datetime.timedelta(hours=1)

        self._flush_to_database(prev_hour)

        return [(self.run, self.next_run(), )]

    def _flush_to_database(self, hour):

        if hour not in self._state["data"]:
            self._log.warn("no data for %s" % (hour, ))
            return

        self._log.info("storing data for %s" % (hour, ))
        space_accounting_database = SpaceAccountingDatabase()
        for collection_id, events in self._state["data"][hour].items():
            space_accounting_database.store_collection_stats(
                collection_id,
                hour,
                events.get("bytes_added", 0),
                events.get("bytes_removed", 0),
                events.get("bytes_retrieved", 0)
            )
        space_accounting_database.commit()

        del self._state["data"][hour]

