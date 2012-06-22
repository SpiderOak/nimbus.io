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

    def encode(self, data_blocks):
        """
        data_blocks
            a list of data blocks to be encoded

        return
            a list size=num_segments of lists of encoded blocks (zfec shares)
        """
        return_list = [list() for _ in range(self.num_segments)]
        encoder = Encoder(self.min_segments, self.num_segments)
        for data_block in data_blocks:
            for segment_list, zfec_share in  zip(return_list, 
                                                 encoder.encode(data_block)):
                segment_list.append(zfec_share)
                
        return return_list

    def decode(self, segments, segment_numbers, padding_size):
        """
        segments
            a list size=num_segments of lists of encoded blocks (zfec shares)
        segment_numbers
            a list of ints giving the segment numbers of the segments (1..n)
        padding_size
            the zfec padding size of the last block (earlier blocks are 0)

        return
            a list of data blocks
        """
        data_list = list()
        decoder = Decoder(self.min_segments, self.num_segments)
        zfec_segment_numbers = [n-1 for n in segment_numbers]

        # accumulate all but the last slice with padding set to 0
        for i in range(len(segments[0])-1):
            encoded_blocks = [segment[i] for segment in segments]
            data_list.append(
                decoder.decode(encoded_blocks, zfec_segment_numbers, 0)
            )
        
        # accumulate the last slice using the padding size
        encoded_blocks = [segment[-1] for segment in segments]
        data_list.append(
            decoder.decode(encoded_blocks, zfec_segment_numbers, padding_size)
        )

        return data_list

