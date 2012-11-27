# -*- coding: utf-8 -*-
"""
redis_sink.py

A Greenlet that acts as a sink: 
reading status entries a queue and passing them to redis
"""
import logging
import os

import gevent.greenlet
import gevent.queue

import redis

_redis_host = os.environ.get("REDIS_HOST", "localhost")
_redis_port = int(os.environ.get("REDIS_PORT", str(6379)))
_redis_db = int(os.environ.get("REDIS_DB", str(0)))

class RedisSink(gevent.greenlet.Greenlet):
    """
    """
    def __init__(self, halt_event, redis_queue):
        gevent.greenlet.Greenlet.__init__(self)
        self._log = logging.getLogger("redis_sink")
        self._halt_event = halt_event
        self._redis_queue = redis_queue
        self._redis_connection = None

    def join(self, timeout=None):
        self._log.info("joining")
        gevent.greenlet.Greenlet.join(self, timeout)

    def _run(self):
        self._log.info("connecting to {0}:{1} db={2}".format(_redis_host, 
                                                             _redis_port, 
                                                             _redis_db))
        self._redis_connection = redis.StrictRedis(host=_redis_host, 
                                                   port=_redis_port, 
                                                   db=_redis_db)

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

