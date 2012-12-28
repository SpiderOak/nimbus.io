# -*- coding: utf-8 -*-
"""
operational_stats_redis_sink.py

A Greenlet that acts as a sink: 
reading status entries a queue and passing them to redis

See Ticket #64 Implement Operational Stats Accumulation
"""
from collections import namedtuple

from tools.redis_sink import RedisSink
from tools.operational_stats_redis_key import compute_key

redis_queue_entry_tuple = namedtuple("RedisQueueEntry", ["timestamp",
                                                         "collection_id",
                                                         "value"])
class OperationalStatsRedisSink(RedisSink):
    """
    A Greenlet that acts as a sink: 
    reading status entries a queue and passing them to redis

    See Ticket #64 Implement Operational Stats Accumulation
    """
    def __init__(self, halt_event, redis_queue, node_name):
        RedisSink.__init__(self, halt_event, redis_queue)
        self._node_name = node_name

    def store(self, partial_key, entry):
        """
        store one entry from the queue in redis
        """
        key = compute_key(self._node_name, entry.timestamp, partial_key)
        self._log.debug("hincrby({0}, {1}, {2})".format(key, 
                                                        entry.collection_id, 
                                                        entry.value))
        self._redis_connection.hincrby(key, entry.collection_id, entry.value)

