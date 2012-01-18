# -*- coding: utf-8 -*-
"""
time_queue.py

A priority queue based on time
"""

import heapq
import time           

class TimeQueue(object):
    """
    a priority queue based on time
    """
    
    def __init__(self):
        """create a time queue"""
        self._queue = list()
        
    def put(self, task, start_time=time.time()):
        """
        Put one task into the queue, to be run no earlier than the specified
        time.

        Defaults to the current time: i.e. task will be run as soon as 
        possible.
        """ 
        assert task is not None
        heapq.heappush(self._queue, (start_time, task, ))
        
    def peek_time(self):
        """
        return the time the next (oldest) task is due
        """
        return self._queue[0][0]
        
    def pop(self):
        """
        remove the next (oldest) task from the queue and return it
        """
        _, task = heapq.heappop(self._queue)
        return task             
        
    def __len__(self):
        """report the size of the queue"""
        return len(self._queue)   
        
                                                                                                                
