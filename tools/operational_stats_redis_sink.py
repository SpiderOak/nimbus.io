# -*- coding: utf-8 -*-
"""
operational_stats_redis_sink.py

A Greenlet that acts as a sink: 
reading status entries a queue and passing them to redis

See Ticket #64 Implement Operational Stats Accumulation
"""
from collections import namedtuple

from tools.redis_sink import RedisSink

redis_queue_entry_tuple = namedtuple("RedisQueueEntry", ["timestamp",
                                                         "collection_id",
                                                         "value"])
key_prefix = "nimbus.io.collection_ops_accounting"

def compute_key(timestamp, partial_key):
    timestamp_template = "{0}.{1:02}.{2:02}.{3:02}.{4:02}"
    timestamp_string = timestamp_template.format(timestamp.year,
                                                 timestamp.month,
                                                 timestamp.day,
                                                 timestamp.hour,
                                                 timestamp.minute)
    return ".".join([key_prefix, timestamp_string, partial_key])

class OperationalStatsRedisSink(RedisSink):
    """
    A Greenlet that acts as a sink: 
    reading status entries a queue and passing them to redis

    See Ticket #64 Implement Operational Stats Accumulation
    """
    def __init__(self, halt_event, redis_queue):
        RedisSink.__init__(self, halt_event, redis_queue)

    def store(self, partial_key, entry):
        """
        store one entry from the queue in redis
        """
        key = compute_key(entry.timestamp, partial_key)
        self._log.debug("hincrby({0}, {1}, {2})".format(key, 
                                                        entry.collection_id, 
                                                        entry.value))
        self._redis_connection.hincrby(key, entry.collection_id, entry.value)

