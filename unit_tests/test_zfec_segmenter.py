# -*- coding: utf-8 -*-
"""
test_unified_id_factory.py
"""
import os
import random
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.data_definitions import block_generator
from web_server.zfec_segmenter import ZfecSegmenter

_min_segments = 8
_num_segments = 10

class TestZfecSegmenter(unittest.TestCase):
    """test the zfec segmenter"""

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        pass

    def test_unpadded_segment(self):
        """test a segment that doesn't need padding"""
        segment_size = 10 * 1024 * 1024
        test_data = os.urandom(segment_size)
        segmenter = ZfecSegmenter(_min_segments, _num_segments)

        padding_size = segmenter.padding_size(test_data)
        encoded_segments = segmenter.encode(block_generator(test_data))

        segment_numbers = range(1, _num_segments+1)

        test_segment_numbers = random.sample(segment_numbers, _min_segments)
        test_segments = [encoded_segments[n-1] for n in test_segment_numbers]

        decoded_segments = segmenter.decode(
            test_segments, test_segment_numbers, padding_size
        )

        decoded_data = "".join(decoded_segments)
        self.assertTrue(decoded_data == test_data, len(decoded_data))

    def test_padded_segment(self):
        """test a segment that needs padding"""
        segment_size = 10 * 1024 * 1024 - 1
        test_data = os.urandom(segment_size)
        segmenter = ZfecSegmenter(_min_segments, _num_segments)

        padding_size = segmenter.padding_size(test_data)
        encoded_segments = segmenter.encode(block_generator(test_data))
        
        segment_numbers = range(1, _num_segments+1)

        test_segment_numbers = random.sample(segment_numbers, _min_segments)
        test_segments = [encoded_segments[n-1] for n in test_segment_numbers]

        decoded_segments = segmenter.decode(
            test_segments, test_segment_numbers, padding_size
        )

        decoded_data = "".join(decoded_segments)
        self.assertTrue(decoded_data == test_data, len(decoded_data))

if __name__ == "__main__":
    unittest.main()

