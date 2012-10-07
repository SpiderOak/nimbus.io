# -*- coding: utf-8 -*-
"""
pending_handoffs.py

a container to hold the pending handoffs for a node, ordered by
unified_id
"""
import heapq
import logging

from gevent.coros import RLock

class PendingHandoffs(object):
    """
    a container to hold the pending handoffs for a node, ordered by
    unified_id
    """
    def __init__(self):
        self._log = logging.getLogger("PendingHandoffs")
        self._list = list()
        self._dict = dict()
        heapq.heapify(self._list)
        self._lock = RLock()
        self._segments_already_seen = set()

    def push(self, incoming_segment_row, source_node_name):
        """
        add the segment row, with its source
        """
        # we want to identify duplicate segments and send them along
        # so the handoff_manager can send a purge request to the 
        # source node
        segment_key = (incoming_segment_row["unified_id"], 
                       incoming_segment_row["conjoined_part"], )
        if segment_key in self._segments_already_seen:
            duplicate = True
        else:
            duplicate = False
            self._segments_already_seen.add(segment_key)

        self._lock.acquire()

        # we may, or may not, have two instances of a segment in the
        # pending queue at the same time (one a duplicate). 
        # We want to give each a unique key
        instance_count = 1
        entry_key = (incoming_segment_row["unified_id"], 
                     incoming_segment_row["conjoined_part"], 
                     instance_count, )
        while entry_key in self._dict:
            instance_count += 1
            entry_key = (incoming_segment_row["unified_id"], 
                         incoming_segment_row["conjoined_part"], 
                         instance_count, )


        heapq.heappush(self._list, entry_key)
        self._dict[entry_key] = (incoming_segment_row, 
                                 source_node_name, 
                                 duplicate, )

        self._lock.release()

    def pop(self):
        """
        return a tuple of (segment_row, source_node_name, duplicate)
        where duplicate will be False for the first instance of the segment
        raise IndexError if there is none
        """
        self._lock.acquire()
        try:
            entry_key = heapq.heappop(self._list)
            return self._dict.pop(entry_key)
        finally:
            self._lock.release()

    def __len__(self):
        return len(self._list)

