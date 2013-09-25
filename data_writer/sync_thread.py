# -*- coding: utf-8 -*-
"""
sync_thread.py

A timer thread to perioidically tell the writer thread to sync the output
value file.
"""
from threading import Thread

_sync_interval = 1.0
_sync_message = {"message-type" : "sync-value-file"}

class SyncThread(Thread):
    """
    A timer thread to perioidically tell the writer thread to sync the output
    value file.
    """
    def __init__(self, halt_event, message_queue):
        Thread.__init__(self, name="SyncThread")
        self._halt_event = halt_event
        self._message_queue = message_queue

    def run(self):
        while not self._halt_event.is_set():
            self._halt_event.wait(_sync_interval)
            self._message_queue.put((_sync_message, None))
