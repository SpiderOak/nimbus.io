# -*- coding: utf-8 -*-
"""
consistency_check_starter.py

A time queue action to periodically start a new consistency check
"""
import datetime
import logging
import os
import random
import time

from diyapi_anti_entropy_server.audit_result_database import \
    AuditResultDatabase

_polling_interval = float(os.environ.get(
    "NIMBUSIO_ANTI_ENTROPY_POLLING_INTERVAL", "1800.0")
)
_audit_cutoff_days = int(os.environ.get("NIMBUSIO_AUDIT_CUTOFF_DAYS", "14"))
_max_active_checks = int(os.environ.get(
    "NIMBUSIO_ANTI_ENTROPY_MAX_ACTIVE_CHECKS", "1")
)

class ConsistencyCheckStarter(object):
    """
    A time queue action to periodically start a new consistency check
    """
    def __init__(self, state, start_consistency_check):
        self._log = logging.getLogger("ConsistencyCheckStarter")
        self._state = state
        self._start_consistency_check = start_consistency_check

    @classmethod
    def next_run(cls):
        return time.time() + _polling_interval

    def run(self, halt_event):
        """pick a collection and start a new consistency check"""
        if halt_event.is_set():
            self._log.info("halt-event is set: exiting")
            return
                
        cutoff_timestamp = \
            datetime.datetime.now() - \
            datetime.timedelta(days=_audit_cutoff_days)
        database = AuditResultDatabase(
            self._state["central-database-connection"]
        )
        ineligible_collection_ids = set(
            database.ineligible_collection_ids(cutoff_timestamp)
        )
        eligible_collection_ids = self._state["collection-ids"] \
                            - ineligible_collection_ids

        self._log.info(
            "found %s collections eligible for consistency check" % (
                len(eligible_collection_ids),
            )
        )

        eligible_collection_id_list = list(eligible_collection_ids)
        while len(eligible_collection_id_list) > 0 \
        and len(self._state["active-requests"]) < _max_active_checks:
            collection_id = random.choice(eligible_collection_id_list)
            eligible_collection_id_list.remove(collection_id)
            self._start_consistency_check(self._state, collection_id)

        return [(self.run, self.next_run(), )]

