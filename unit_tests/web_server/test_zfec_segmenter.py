# -*- coding: utf-8 -*-
"""
test_zfec_segmenter.py

test diyapi_web_server/zfec_segmenter.py
"""
import unittest

from unit_tests.util import random_string

from diyapi_web_server.zfec_segmenter import ZfecSegmenter


NUM_SEGMENTS = 10
MIN_SEGMENTS = 8


class TestZfecSegmenter(unittest.TestCase):
    """test diyapi_web_server/zfec_segmenter.py"""
    def setUp(self):
        self.segmenter = ZfecSegmenter(MIN_SEGMENTS, NUM_SEGMENTS)

    def test_encode(self):
        data = random_string(1024)

        segments = self.segmenter.encode(data)

        self.assertEqual(len(segments), NUM_SEGMENTS)

        header, segment_data = self.segmenter._parse_segment(segments[0])
        self.assertEqual(header.padlen,
                         MIN_SEGMENTS * len(segment_data) - len(data))

    def test_decode(self):
        data = random_string(1024)
        segments = self.segmenter.encode(data)

        decoded_data = self.segmenter.decode(segments)

        self.assertEqual(decoded_data, data)


if __name__ == "__main__":
    unittest.main()
