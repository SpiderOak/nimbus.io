# -*- coding: utf-8 -*-
"""
avatar_list_requestor.py

A time queue action to periodically request a list of avatars
"""
import logging
import os
import random
import time
import uuid

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
        request a list of avatar ids from a random database server.
        We could track down the local server, but we're not going to do this
        very often.
        """
        if halt_event.is_set():
            self._log.info("halt-event is set, exiting")
            return
                
        request_id = uuid.uuid1().hex

        self._log.info("requesing list of avatar ids from %s" % (request_id, ))
        request = {
            "message-type"  : "avatar-list-request",
            "request-id"    : request_id,
        }

        database_client = random.choice(self._state["database-clients"])
        database_client.queue_message_for_send(request)

        return [(self.run, self.next_run(), )]

