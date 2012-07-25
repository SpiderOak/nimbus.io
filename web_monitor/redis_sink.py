# -*- coding: utf-8 -*-
"""
redis_sink.py

A Greenlet that acts as a sink: reading status updates from a queue and storing
them in a resis
"""
import logging
import os
import socket

import gevent.greenlet
import gevent.queue

import redis

_redis_host = os.environ.get("REDIS_HOST", "localhost")
_redis_port = int(os.environ.get("REDIS_PORT", str(6379)))
_redis_db = int(os.environ.get("REDIS_DB", str(0)))
_hostname = socket.gethosthame()
_hash_name = "nimbus.io.web_monitor.{0}".format(_hostname)

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
                entry = self._redis_queue.get(block=True, timeout=1.0)
            except gevent.queue.Empty:
                continue

        self._log.debug("end halt_event loop")

        self._redis_connection.shutdown()

