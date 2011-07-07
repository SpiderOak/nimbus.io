# -*- coding: utf-8 -*-
"""
avatar_list_requestor.py

A time queue action to periodically request a list of avatars
"""
import logging
import os
import time

_avatar_polling_interval = float(os.environ.get(
    "DIYAPI_ANTI_ENTROPY_AVATAR_POLLING_INTERVAL", "86400.0") # 24 * 60 * 60
)

class AvatarListRequestor(object):
    """
    A time queue action to periodically request a list of avatars
    """
    def __init__(self, state):
        self._log = logging.getLogger("AvatarListRequestor")
        self._state = state

    @classmethod
    def next_run(cls):
        return time.time() + _avatar_polling_interval

    def run(self, halt_event):
        """
        request a list of avatar ids from the local database
        """
        if halt_event.is_set():
            self._log.info("halt-event is set, exiting")
            return

        avatar_id_generator = \
                self._state["local-database-connection"].generate_all_rows(
                    """select distinct avatar_id from diy.segment"""
                )
        for (avatar_id, ) in avatar_id_generator:
            self._state["avatar-ids"].add(avatar_id)

        self._log.info("%s known avatar ids" % (
            len(self._state["avatar-ids"]), 
        ))
                
        return [(self.run, self.next_run(), )]

