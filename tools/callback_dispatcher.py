# -*- coding: utf-8 -*-
"""
callback_dispatcher.py

a time_queue object to dispatch to a single callback function
"""
import logging
import time

class CallbackDispatcher(object):
    """
    a time_queue object to dispatch to a single callback function
    """    
    def __init__( self, state, input_queue, callback, polling_interval=1.0):
        self._log = logging.getLogger("callback_dispatcher")
        self._state = state
        self._input_queue = input_queue
        self._callback = callback
        self._polling_interval = polling_interval

    def run(self, halt_event):
        """
        dispatch every message from the input queue
        This is a task for the time queue
        """        
        if halt_event.is_set():
            self._log.info("halt_event set: exiting")
            return
       
        message_count = 0
        while True:
            try:
                message, _ = self._input_queue.popleft()
            except IndexError:
                break

            message_count += 1
            self._callback(self._state, message, None)
                
        next_interval = (self._polling_interval if message_count == 0 else 0.0)

        return [(self.run, time.time() + next_interval, ), ]

