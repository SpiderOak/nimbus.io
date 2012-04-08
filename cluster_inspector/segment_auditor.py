# -*- coding: utf-8 -*-
"""
segment_auditor.py

audit segment data retrievecd from node databases
"""
import logging

class SegmentAuditor(object):
    """
    audit segment data retrievecd from node databases
    """
    def __init__(self, state, work_path):
        self._log = logging.getLogger("SegmentAuditor")
        self._state = state
        self._work_path = work_path

    def run(self, halt_event):
        """
        process segment data
        """
        self._log.info("audit started")

        # set the halt event when we are done
        self._log.info("audit completed")
        halt_event.set()

