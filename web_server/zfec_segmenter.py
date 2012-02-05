# -*- coding: utf-8 -*-
"""
zfec_segmenter.py

Encodes/decodes segments using zfec.
"""
from zfec.easyfec import Encoder, Decoder

from tools.data_definitions import block_size, encoded_block_slice_size

def _slice_count(data_size, slice_size):
    if data_size == 0:
        return 0
    if data_size % slice_size == 0:
        return data_size / slice_size
    return (data_size / slice_size) + 1

def _slice_generator(data, slice_size):
    start_pos = 0
    end_pos = slice_size
    while start_pos < len(data):
        yield data[start_pos:end_pos]
        start_pos = end_pos
        end_pos += slice_size

class ZfecSegmenter(object):
    def __init__(self, min_segments, num_segments):
        self.min_segments = min_segments
        self.num_segments = num_segments

    def padding_size(self, data):
        modulus = len(data) % self.min_segments
        return (0 if modulus == 0 else self.min_segments - modulus)

    def encode(self, data):
        """
        data
            data to be encoded

        return
            a list of num_segments of encoded zfec shares concatenated
        """
        return_list = [list() for _ in range(self.num_segments)]
        encoder = Encoder(self.min_segments, self.num_segments)
        for data_slice in _slice_generator(data, block_size):
            for segment_list, zfec_share in  zip(
                return_list, encoder.encode(data_slice)
            ):
                segment_list.append(zfec_share)
                
        return ["".join(sublist) for sublist in return_list]

    def decode(self, segments, segment_numbers, padding_size):
        """
        segments
            a list of num_segments strings each containing concatenated
            encoded blocks
        segment_numbers
            a list of ints giving the segment nimbers of the segments (1..n)
        padding_size
            the zfec padding size of the last block (earlier blocks are 0)
        """
        data_list = list()
        slice_count = _slice_count(len(segments[0]), encoded_block_slice_size)
        slicegens = [
            _slice_generator(s, encoded_block_slice_size) for s in segments
        ]
        decoder = Decoder(self.min_segments, self.num_segments)
        zfec_segment_numbers = [n-1 for n in segment_numbers]

        # accumulate all but the last slice with padding set to 0
        for _ in range(slice_count-1):
            encoded_blocks = [slicegen.next() for slicegen in slicegens]
            data_list.append(
                decoder.decode(encoded_blocks, zfec_segment_numbers, 0)
            )
        
        # accumulate the last slice using the padding size
        encoded_blocks = [slicegen.next() for slicegen in slicegens]
        data_list.append(
            decoder.decode(encoded_blocks, zfec_segment_numbers, padding_size)
        )

        return "".join(data_list)

