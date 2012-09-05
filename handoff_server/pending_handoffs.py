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

    def push(self, incoming_segment_row, source_node_name):
        """
        add the segment row, with its source
        """
        self._lock.acquire()
        handoff_key = (incoming_segment_row["unified_id"], 
                       incoming_segment_row["conjoined_part"], )
        if handoff_key in self._dict:
            segment_row, source_node_names = self._dict[handoff_key]
            assert incoming_segment_row["collection_id"] == \
                segment_row["collection_id"], (
                incoming_segment_row, segment_row,
            )
            assert incoming_segment_row["key"] == segment_row["key"], (
                incoming_segment_row, segment_row,
            )
            if source_node_name in source_node_names:
                self._log.warn("duplicate: {0} {1}".format(
                    incoming_segment_row, source_node_name))
            else:
                source_node_names.append(source_node_name)
                assert len(source_node_names) <= 2
        else:
            heapq.heappush(self._list, handoff_key)
            self._dict[handoff_key] = (
                incoming_segment_row, [source_node_name, ], 
            )            
        self._lock.release()

    def pop(self):
        """
        return a tuple of the oldest segment row with a list of its sources
        raise IndexError if there is none
        """
        self._lock.acquire()
        try:
            handoff_key = heapq.heappop(self._list)
            return self._dict.pop(handoff_key)
        finally:
            self._lock.release()

    def __len__(self):
        return len(self._list)

