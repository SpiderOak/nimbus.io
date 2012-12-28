# -*- coding: utf-8 -*-
"""
redis_sink.py

A Greenlet that acts as a sink: 
reading status entries a queue and passing them to redis
"""
import logging

import gevent.greenlet
import gevent.queue

from tools.redis_connection import create_redis_connection

class RedisSink(gevent.greenlet.Greenlet):
    """
    """
    def __init__(self, halt_event, redis_queue):
        gevent.greenlet.Greenlet.__init__(self)
        self._name = "redis_sink"
        self._log = logging.getLogger(self._name)
        self._halt_event = halt_event
        self._redis_queue = redis_queue
        self._redis_connection = None

    def __str__(self):
        return self._name

    def join(self, timeout=None):
        self._log.info("joining")
        gevent.greenlet.Greenlet.join(self, timeout)

    def _run(self):
        self._redis_connection = create_redis_connection()

        self._log.debug("start halt_event loop")
        while not self._halt_event.is_set():
            try:
                key, entry = self._redis_queue.get(block=True, timeout=1.0)
            except gevent.queue.Empty:
                continue
            self.store(key, entry)

        self._log.debug("end halt_event loop")

    def store(self, key, entry):
        """
        store one entry from the queue in redis
        """
        # we expect a derived class to implement this

