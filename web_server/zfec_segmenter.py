# -*- coding: utf-8 -*-
"""
zfec_segmenter.py

Encodes/decodes segments using zfec.
"""
from zfec.easyfec import Encoder, Decoder

class ZfecSegmenter(object):
    def __init__(self, min_segments, num_segments):
        self.min_segments = min_segments
        self.num_segments = num_segments

    def padding_size(self, data):
        modulus = len(data) % self.min_segments
        return (0 if modulus == 0 else self.min_segments - modulus)

    def encode(self, data):
        encoder = Encoder(self.min_segments, self.num_segments)
        return encoder.encode(data)

    def decode(self, segments, segment_numbers, padding_size):
        decoder = Decoder(self.min_segments, self.num_segments)
        return decoder.decode(
            segments, [n-1 for n in segment_numbers], padding_size
        )

