# -*- coding: utf-8 -*-
"""
state_cleaner.py

A time queue action to perioidcially clean out the state
"""
import logging
import time

from diyapi_anti_entropy_server.audit_result_database import \
        AuditResultDatabase
from diyapi_anti_entropy_server.common import retry_interval, \
        retry_time, \
        retry_entry_tuple, \
        max_retry_count

_polling_interval = 5.0 * 60.0

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
            self._log.info("halt-event is set: exiting")
            return
                
        current_time = time.time()
        expired_requests = list()
        # see if we have any timeouts
        for request_id, request_state in self._state["active-requests"]:
            if current_time > request_state.timeout:
                self._log.warn(
                    "%s timed out waiting message" % (
                        request_id
                    )
                )   
                expired_requests.append(request_id)
                self._timeout(request_state)

        for request_id in expired_requests:
            del self._state["active-requests"][request_id]

        return [(self.run, self.next_run(), )]

    def _timeout(self, request_state):
        """
        If we don't hear from all the nodes in a reasonable time,
        put the request in the retry queue
        """
        database = AuditResultDatabase()

        if request_state.retry_count >= max_retry_count:
            self._log.error("timeout: %s with too many retries %s " % (
                request_state.avatar_id, request_state.retry_count
            ))
            database.too_many_retries(request_state.row_id)
            database.close()
            # TODO: need to do something with this
            return

        self._log.error("timeout %s. will retry in %s seconds" % (
            request_state.avatar_id, retry_interval,
        ))
        self._state["retry-list"].append(
            retry_entry_tuple(
                retry_time=retry_time(), 
                avatar_id=request_state.avatar_id,
                row_id=request_state.row_id,
                retry_count=request_state.retry_count, 
            )
        )
        database.wait_for_retry(request_state.row_id)
        database.close()

