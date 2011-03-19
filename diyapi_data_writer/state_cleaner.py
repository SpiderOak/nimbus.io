# -*- coding: utf-8 -*-
"""
state_cleaner.py

A time queue action to perioidcially clean out the state
"""
import logging
import os
import time

_polling_interval = 10.0

class StateCleaner(object):
    """A time queue action to periodically clean out the state"""
    def __init__(self, state, start_block_requests):
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
        for request_id, request_state in self._state["active-requests"].items():
            if halt_event.is_set():
                return
            if current_time > request_state.timeout:
                overdue_requests.append(request_id)

        for request_id in overdue_requests:        
            if halt_event.is_set():
                return

            request_state = self._state["active-requests"].pop(request_id)
            log.warn("%s timed out waiting reply %s" % ( 
                request_id, request_state
            ))

            if request_state.timeout_message is not None:
                self._send_timeout_message(request_id, request_state)

        return [(self.run, self.next_run(), )]


    def _send_timeout_message(self, request_id, request_state):
        reply = {
            "message-type"  : request_state.timeout_message,
            "xrep-ident"    : request_state.xrep_ident,
            "request-id"    : request_id,
            "result"        : "timed-out",
            "error-message" : "timed out waiting for message reply",
        }
        self._state["xrep-server"].queue_message_for_send(reply)

