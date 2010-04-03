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

    def test_slicer(self):
        data = [random_string(SLICE_SIZE),
                random_string(SLICE_SIZE),
                'extra data at the end']
        f = StringIO(''.join(data))
        slicer = DataSlicer(f, SLICE_SIZE)
        yielded_data = list(slicer)
        self.assertEqual(yielded_data, data)


if __name__ == "__main__":
    unittest.main()
