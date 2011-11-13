# -*- coding: utf-8 -*-
"""
queue_dispatcher.py

a time_queue object to dispatch to functions based on message-type
"""
import logging
import time

class DequeDispatcher(object):
    """
    a time_queue task to which pops messages from a deque and uses
    a dispatch table to call functions based on message-type

    state
        the internal state object, passed to dispatch functions

    input_queue
        a deque of incoming messages from various zeromq sockets

    dispatch_table
        a dictionary mapping message_type to a callback function

    polling_interval (optional)
        how often this dspatcher is called by the event loop

    default_handler
        A function to be called if message_type is not explicitly mapped
        in the dispatch table.
    """    
    def __init__(
        self, 
        state, 
        input_queue, 
        dispatch_table, 
        polling_interval=1.0,
        default_handler=None
    ):
        self._log = logging.getLogger("deque_dispatcher")
        self._state = state
        self._input_queue = input_queue
        self._dispatch_table = dispatch_table
        self._polling_interval = polling_interval
        self._default_handler = default_handler

    def run(self, halt_event):
        """
        This is a task for the time queue

        dispatch one message from the input queue to a callback function

        the function is called with ``(state, message, data)``

        the function can return a list of tasks to be added to the time queue
        """        
        if halt_event.is_set():
            self._log.info("halt_event set: exiting")
            return
        
        next_tasks = list()

        try:
            message, data = self._input_queue.popleft()
        except IndexError:
            next_interval = time.time() + self._polling_interval
        else:
            result = self._dispatch_message(message, data)
            if result is not None:
                next_tasks.extend(result)
            next_interval = time.time()

        # put ourselves into the time queue to run at the next polling interval
        next_tasks.append((self.run, next_interval, ))
        return next_tasks

    def _dispatch_message(self, message, data):
        handler = self._default_handler

        try:
            handler = self._dispatch_table[message["message-type"]]
        except KeyError:
            pass

        if handler is None:
            self._log.error("Unknown message type: %s" % (message,))
            return None

        return handler(self._state, message, data)

