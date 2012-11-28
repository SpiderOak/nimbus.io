# -*- coding: utf-8 -*-
"""
test_stats_accumulation.py

Test queueing stats information
See Ticket #64 Implement Operational Stats Accumulation
"""
from datetime import datetime, timedelta
import logging
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from gevent import monkey
# you must use the latest gevent and have c-ares installed for this to work
# with /etc/hosts 
# hg clone https://bitbucket.org/denis/gevent
monkey.patch_all()

import gevent
import gevent.queue
from gevent.event import Event

from tools.redis_connection import create_redis_connection
from tools.operational_stats_redis_sink import OperationalStatsRedisSink, \
    key_prefix, \
    queue_entry_tuple, \
    compute_key

def _initialize_logging_to_stderr():
    from tools.standard_logging import _log_format_template
    log_level = logging.DEBUG
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _unhandled_greenlet_exception(greenlet_object):
    log = logging.getLogger("_unhandled_greenlet_exception")
    error_message = "{0} {1} {2}".format(
        str(greenlet_object),
        greenlet_object.exception.__class__.__name__,
        str(greenlet_object.exception))
    log.error(error_message)

class TestStatsAccumulator(unittest.TestCase):
    """
    test StatsAccumulator
    """
    def setUp(self):
        self._halt_event = Event()
        self._redis_queue = gevent.queue.Queue()
        self._redis_sink = OperationalStatsRedisSink(self._halt_event, 
                                                     self._redis_queue)
        self._redis_sink.link_exception(_unhandled_greenlet_exception)
        self._redis_sink.start()
        self._redis_connection = create_redis_connection()

    def tearDown(self):
        log = logging.getLogger("teardown")
        log.info("1")
        if hasattr(self, "_halt_event"):
            self._halt_event.set()
        log.info("2")
        if hasattr(self, "redis_sink"):
            self._redis_sink.join()
        log.info("3")
        if hasattr(self, "_redis_queue"):
            delattr(self, "_redis_queue")
        log.info("4")
        if hasattr(self, "_halt_event"):
            delattr(self, "_halt_event")
        log.info("5")
        if hasattr(self, "redis_sink"):
            delattr(self, "redis_sink")
        log.info("6")
        for key in self._redis_connection.keys("{0}.*".format(key_prefix)):
            self._redis_connection.delete(key)
        if hasattr(self, "_redis_connection"):
            delattr(self, "_redis_connection")
        log.info("7")

    def test_greenlet_creation(self):
        """
        test that we can create, and stop, the greenlet
        """
        keys = self._redis_connection.keys()
        print >> sys.stderr, "keys =", str(keys)

        # give the greenlet some time to start
        self._halt_event.wait(1)

    def test_single_increment(self):
        """
        test that we can increment a key once
        """
        partial_key = "get_request"
        queue_entry = queue_entry_tuple(timestamp=datetime.utcnow(),
                                        collection_id=42,
                                        value=12345)

        # feed an entry to the queue
        self._redis_queue.put((partial_key, queue_entry), )

        # give the greenlet some time to start
        self._halt_event.wait(1)

        # verify that the key got incremented
        expected_key = compute_key(queue_entry.timestamp,
                                   partial_key)
        hash_value = self._redis_connection.hget(expected_key,
                                                 queue_entry.collection_id)
        self.assertEqual(int(hash_value), queue_entry.value)

    def test_multiple_increment(self):
        """
        test that we can increment a key multiple times in a time interval
        """
        partial_key = "get_request"

        # create a base time, rounded off to the nearest minute
        current_time = datetime.utcnow()
        base_time = datetime(current_time.year,
                             current_time.month,
                             current_time.day,
                             current_time.hour,
                             current_time.minute)

        test_range = range(10)
        for index in test_range:
            timestamp = base_time + timedelta(seconds=index)
            queue_entry = queue_entry_tuple(timestamp=timestamp,
                                            collection_id=42,
                                            value=index)

            self._redis_queue.put((partial_key, queue_entry), )

        # give the greenlet and redis some time to operate
        self._halt_event.wait(1)

        # verify that the key got incremented
        expected_key = compute_key(queue_entry.timestamp,
                                   partial_key)
        expected_value = sum([x for x in test_range])
        hash_value = self._redis_connection.hget(expected_key,
                                                 queue_entry.collection_id)
        self.assertEqual(int(hash_value), expected_value)

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    unittest.main()

