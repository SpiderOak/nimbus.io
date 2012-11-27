# -*- coding: utf-8 -*-
"""
test_stats_accumulation.py

Test queueing stats information
See Ticket #64 Implement Operational Stats Accumulation
"""
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
from  gevent.greenlet import Greenlet

class TestStatsAccumulator(unittest.TestCase):
    """
    test StatsAccumulator
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_key_creation(self):
        """
        test that we create the correct key
        """
        self.assertEqual(1, 0)

if __name__ == "__main__":
    unittest.main()

