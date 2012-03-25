# -*- coding: utf-8 -*-
"""
pending_handoffs.py

a container to hold the pending handoffs for a node, ordered by
unified_id
"""
import heapq
import logging

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

    def push(self, incoming_segment_row, source_node_name):
        """
        add the segment row, with its source
        """
        if incoming_segment_row.unified_id in self._dict:
            segment_row, source_node_names = \
                    self._dict[incoming_segment_row.unified_id]
            assert incoming_segment_row.collection_id == \
                segment_row.collection_id, (
                incoming_segment_row, segment_row,
            )
            assert incoming_segment_row.key == segment_row.key, (
                incoming_segment_row, segment_row,
            )
            if source_node_name in source_node_names:
                self._log.debug("duplicate: %s %s" % (
                    incoming_segment_row, source_node_name,
                ))
            else:
                source_node_names.append(source_node_name)
        else:
            heapq.heappush(self._list, incoming_segment_row.unified_id)
            self._dict[incoming_segment_row.unified_id] = (
                incoming_segment_row, [source_node_name, ], 
            )            

    def pop(self):
        """
        return a tuple of the oldest segment row wiht a list of its sources
        raise IndexError if there is none
        """
        unified_id = heapq.heappop(self._list)
        return self._dict.pop(unified_id)

    def __len__(self):
        return len(self._list)

