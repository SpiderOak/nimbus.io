# -*- coding: utf-8 -*-
"""
zfec_segmenter.py

Encodes/decodes segments using zfec.
"""
import struct
from collections import namedtuple

from zfec.easyfec import Encoder, Decoder


class ZfecSegmenter(object):
    _header_tuple = namedtuple('Header', [
        'padlen',       # I
        'share_num',    # B
    ])
    _header_format = 'IB'
    _header_length = struct.calcsize(_header_format)

    def __init__(self, min_segments, num_segments):
        self.min_segments = min_segments
        self.num_segments = num_segments

    def _parse_segment(self, segment):
        header = self._header_tuple._make(struct.unpack(
            self._header_format, segment[:self._header_length]))
        return header, segment[self._header_length:]

    def _unparse_segment(self, header, segment_data):
        packed_header = struct.pack(
            self._header_format,
            header.padlen,
            header.share_num,
        )
        return packed_header + segment_data

    def encode(self, data):
        encoder = Encoder(self.min_segments, self.num_segments)
        return [
            self._unparse_segment(
                self._header_tuple(
                    self.min_segments * len(segment_data) - len(data),
                    share_num
                ),
                segment_data
            ) for share_num, segment_data in enumerate(encoder.encode(data))
        ]

    def decode(self, segments):
        parsed = map(self._parse_segment, segments[:self.min_segments])
        decoder = Decoder(self.min_segments, self.num_segments)
        return decoder.decode(
            [segment_data for header, segment_data in parsed],
            [header.share_num for header, segment_data in parsed],
            parsed[0][0].padlen
        )
