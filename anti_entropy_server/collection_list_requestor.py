# -*- coding: utf-8 -*-
"""
collection_list_requestor.py

A time queue action to periodically request a list of collections
"""
import logging
import os
import time

_collection_polling_interval = float(os.environ.get(
    "NIMBUSIO_ANTI_ENTROPY_COLLECTION_POLLING_INTERVAL", "86400.0")
)

class CollectionListRequestor(object):
    """
    A time queue action to periodically request a list of collections
    """
    def __init__(self, state):
        self._log = logging.getLogger("CollectionListRequestor")
        self._state = state

    @classmethod
    def next_run(cls):
        return time.time() + _collection_polling_interval

    def run(self, halt_event):
        """
        request a list of collection ids from the local database
        """
        if halt_event.is_set():
            self._log.info("halt-event is set, exiting")
            return

        collection_id_generator = \
                self._state["central-database-connection"].generate_all_rows(
                    """
                    select id 
                    from nimbusio_central.collection
                    where deletion_time is null
                    """
                )
        for (collection_id, ) in collection_id_generator:
            self._state["collection-ids"].add(collection_id)

        self._log.info("%s known collection ids" % (
            len(self._state["collection-ids"]), 
        ))
                
        return [(self.run, self.next_run(), )]

