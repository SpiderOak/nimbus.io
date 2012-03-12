# -*- coding: utf-8 -*-
"""
notifier.py

A time queue action to send archive-key-final messages
when all related output value files have been synced
"""
import logging
import time

_polling_interval = 1.0

class Notifier(object):
    """
    A time queue action to send archive-key-final messages
    when all related output value files have been synced
    """
    def __init__(self, 
                 notification_dependencies, 
                 pending_replies,
                 writer, 
                 resilient_server):
        self._log = logging.getLogger("Notifier")
        self._notification_dependencies = notification_dependencies
        self._pending_replies = pending_replies
        self._writer = writer
        self._resilient_server = resilient_server

        self._log.info(
            "created with polling interval %s" % (_polling_interval)
        )

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        if halt_event.is_set():
            self._log.info("Halt event is set, exiting")
            return

        completed_keys = list()
        for state_key, dependencies in self._notification_dependencies.items():

            # if we depend on the current output value file, and it is not
            # synced, don't send the notification
            if self._writer.value_file_hash in dependencies and \
               not self._writer.value_file_is_synced:
                continue

            message = self._pending_replies.pop(state_key)
            self._resilient_server.send_reply(message)
            completed_keys.append(state_key)

        for state_key in completed_keys:
            del self._notification_dependencies[state_key]

        if len(completed_keys) > 0:
            self._log.info("send {0} replies".format(len(completed_keys)))

        return [(self.run, self.next_run(), )]

