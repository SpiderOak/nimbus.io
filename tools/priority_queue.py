# -*- coding: utf-8 -*-
"""
An object that acts like a deque, but which returns messages in order of
priority (lowest first)
"""

import heapq
import itertools
import logging

from tools.data_definitions import create_priority

class PriorityQueue(object):
    """
    This object is a replacement for dequeue as a receive-queue.
    
    It quacks like a deque, but returns messages in order of 
    message["priority"], lowest first
    """
    def __init__(self):
        self._log = logging.getLogger("PriorityQeueu")
        self._internal_queue = list()
        heapq.heapify(self._internal_queue)
        self._counter = itertools.count()

    def append(self, message_tuple):
        """
        add a message to the queue

        The message must be created by tools.data_definitions.message_format

        It must have a message["priority"] entry
        """
        try:
            priority = message_tuple[0]["priority"]
        except KeyError:
            self._log.error("message lacks priority %s" % (message_tuple[0], ))
            priority = create_priority()

        heapq.heappush(
            self._internal_queue, 
            (priority, self._counter.next(), message_tuple, )
        )

    def popleft(self):
        """
        return the next message in order

        raise IndexError when queue is empty
        """
        _, __, (message, body, ) = heapq.heappop(self._internal_queue)

        return message, body

    def __len__(self):
        return len(self._internal_queue)

