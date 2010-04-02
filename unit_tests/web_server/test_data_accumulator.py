# -*- coding: utf-8 -*-
"""
test_data_accumulator.py

test diyapi_web_server/data_accumulator.py
"""
import unittest
from cStringIO import StringIO

from unit_tests.util import random_string

from diyapi_web_server.data_accumulator import DataAccumulator


SLICE_SIZE = 1024 * 1024


class FakeAccumulatorListener(object):
    def __init__(self):
        self.data = []

    def handle_slice(self, data):
        self.data.append(data)


class TestDataAccumulator(unittest.TestCase):
    """test diyapi_web_server/data_accumulator.py"""

    def test_accumulate(self):
        data = [random_string(SLICE_SIZE),
                random_string(SLICE_SIZE),
                'extra data at the end']
        f = StringIO(''.join(data))
        listener = FakeAccumulatorListener()
        accumulator = DataAccumulator(f, SLICE_SIZE, listener)
        accumulator.accumulate()
        self.assertEqual(listener.data, data)


if __name__ == "__main__":
    unittest.main()
