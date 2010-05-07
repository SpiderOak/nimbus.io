# -*- coding: utf-8 -*-
"""
test_data_slicer.py

test diyapi_web_server/data_slicer.py
"""
import unittest
from cStringIO import StringIO

from unit_tests.util import random_string

from diyapi_web_server.data_slicer import DataSlicer


SLICE_SIZE = 1024 * 1024


class TestDataSlicer(unittest.TestCase):
    """test diyapi_web_server/data_slicer.py"""

    def test_slicer_with_exact_amount(self):
        data = [random_string(SLICE_SIZE),
                random_string(SLICE_SIZE)]
        f = StringIO(''.join(data))
        slicer = DataSlicer(f, SLICE_SIZE, SLICE_SIZE * 2)
        yielded_data = list(slicer)
        self.assertEqual(yielded_data, data)

    def test_slicer_with_extra_data(self):
        data = [random_string(SLICE_SIZE),
                random_string(SLICE_SIZE),
                'extra data at the end']
        f = StringIO(''.join(data) + 'more garbage')
        slicer = DataSlicer(f, SLICE_SIZE, len(''.join(data)))
        yielded_data = list(slicer)
        self.assertEqual(yielded_data, data)


if __name__ == "__main__":
    unittest.main()
