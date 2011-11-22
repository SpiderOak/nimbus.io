# -*- coding: utf-8 -*-
"""
deliverator.py

The deliverator holds the channels that will be used to deliver 
the replies that come over a resilient connection
"""
import logging

from gevent.queue import Queue
from gevent.coros import RLock

class Deliverator(object):
    """
    The deliverator holds the channels that will be used to deliver 
    the replies that come over a resilient connection
    """
    def __init__(self):
        self._log = logging.getLogger("Deliverator")
        self._active_requests = dict()
        self._lock = RLock()

    def add_request(self, message_id):
        """
        Add a message_id

        return a channel (gevent.queue.Queue)

        When the web_server's pull server gets a reply for this message id
        it will push the message into the queue. The caller can block on the
        queue, waiting for the reply.

        we can't use the zero size 'channel' queue because the web server moves 
        on after 8 of 10 retrieves and nobody is waiting on the last two.

        So we use a size of one, and it is the caller's responsibility to clean
        up unused channels.
        """
        channel = Queue(maxsize=1)

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
        except KeyError:
            channel = None
        finally:
            self._lock.release()
        
        if channel is None:
            self._log.error("undeliverable message %s" % (message.control, ))
        else:
            channel.put(message)

