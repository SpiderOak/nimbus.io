# -*- coding: utf-8 -*-
"""
deliverator.py

The deliverator holds the channels that will be used to deliver 
the replies that come over a resilient connection
"""

from gevent.queue import Queue
from gevent.coros import RLock

class Deliverator(object):
    """
    The deliverator holds the channels that will be used to deliver 
    the replies that come over a resilient connection
    """
    def __init__(self):
        self._active_requests = dict()
        self._lock = RLock()

    def add_request(self, message_id):
        """
        Add a message_id
        return a channel that will deliver the reply message 
        """
        channel = Queue(maxsize=0)

        self._lock.acquire()
        try:
            if message_id in self._active_requests:
                raise ValueError("Duplicate request '%s'" % (message_id, ))
            self._active_requests[message_id] = channel
        finally:
            self._lock.release()

        return channel

    def deliver_reply(self, message):
        """
        Deliver the reply nessage over the channel for its message-id
        And discard the channel
        raise KeyError if there is no channel for the request
        """
        self._lock.acquire()
        try:
            channel = self._active_requests.pop(message.control["message-id"])
        finally:
            self._lock.release()

        channel.put(message)

