# -*- coding: utf-8 -*-
"""
web_monitor_redis_sink.py

A Greenlet that acts as a sink: 
reading status entries a queue and passing them to redis
"""
import json
import socket

from tools.redis_sink import RedisSink

_hostname = socket.gethostname()
_hash_name = "nimbus.io.web_monitor.{0}".format(_hostname)

class WebMonitorRedisSink(RedisSink):
    """
    redis sink for web monitor
    """
    def __init__(self, halt_event, redis_queue):
        RedisSink.__init__(self, halt_event, redis_queue)
        self._log.info("hash name = '{0}'".format(_hash_name))

    def store(self, key, entry):
        """
        store one entry from the queue in redis
        """
        # 2012-08-16 dougfort Ticket #29 - format json for debuging
        self._redis_connection.hset(_hash_name, 
                                    key, 
                                    json.dumps(entry, 
                                               sort_keys=True,
                                               indent=4))

