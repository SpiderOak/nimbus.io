# -*- coding: utf-8 -*-
"""
unified_id_factory.py

generate unique ids
"""
import time 

_max_shard_id  = 0b1111111111111 # 13 bits for shard id

#XXX could use the founding of SpiderOak
_instagram_epoch = 1314220021721

class UnifiedIDFactory(object):
    """
    Based on the instagram sharded ids

    http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram

    41 bits for time in milliseconds 
       (gives us 41 years of IDs with a custom epoch)
    13 bits that represent the logical shard ID
    10 bits that represent an auto-incrementing sequence, modulus 1024. 
       This means we can generate 1024 IDs, per shard, per millisecond
    """
    def __init__(self, shard_id):
        if shard_id > _max_shard_id:
            raise ValueError("shard_id {0} exceeds {1}".format(
                shard_id, _max_shard_id
            ))
        self._shifted_shard_id = shard_id << (64-41-13)
        self._prev_millisecond = 0
        self._sequence = 0

    def __iter__(self):
        return self

    def next(self):
        """
        return the next id
        """
        current_millisecond = int(round(time.time() * 1000.0))
        if current_millisecond != self._prev_millisecond:
            self._sequence = 0
            self._prev_millisecond = current_millisecond

        milliseconds_since_epoch = current_millisecond - _instagram_epoch

        # fill the left-most 41 bits into an unsigned integer
        next_id = milliseconds_since_epoch << (64 - 41)

        # fill the next 13 bits with shard-id
        next_id |= self._shifted_shard_id

        # use our sequence to fill out the remaining bits.
        # mod by 1024 (so it fits in 10 bits)
        self._sequence += 1
        next_id |= (self._sequence % 1024)

        return next_id

