# -*- coding: utf-8 -*-
"""
state_cleaner.py

A time queue action to periodically clean out the state
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
        for state_key, request_state in self._state["active-requests"].items():
            if halt_event.is_set():
                return
            if current_time > request_state.timeout:
                overdue_requests.append(state_key)

        for state_key in overdue_requests:        
            if halt_event.is_set():
                return

            request_state = self._state["active-requests"].pop(state_key)
            self._log.warn("%s timed out waiting reply %s" % ( 
                state_key, request_state
            ))

            if request_state.timeout_message is not None:
                self._send_timeout_message(state_key, request_state)

        return [(self.run, self.next_run(), )]


    def _send_timeout_message(self, state_key, request_state):
        avatar_id, key = state_key
        reply = {
            "message-type"  : request_state.timeout_message,
            "message-id"    : request_state.message_id,
            "avatar-id"     : avatar_id,
            "key"           : key,
            "result"        : "timed-out",
            "error-message" : "timed out waiting for message reply",
        }
        self._state["resilient-server"].send_reply(reply)


