# -*- coding: utf-8 -*-
"""
amqp_exchange_manager.py

A class that keeps track of which exchanges are responding.
"""
import os
import random


HANDOFF_NUM = 2


class AMQPExchangeManager(object):
    def __init__(self, exchanges, min_exchanges):
        self.exchanges = list(exchanges)
        # TODO: this probably isn't a good place for min_exchanges, as we
        # really care about min_segments - we could potentially get enough
        # segments to decode data with fewer than min_segments nodes up (hinted
        # handoff)
        self.num_exchanges = len(self.exchanges)
        self.min_exchanges = min_exchanges
        self._down = set()

    def __len__(self):
        return len(self.exchanges) - len(self._down)

    def __iter__(self):
        for i, exchange in enumerate(self.exchanges):
            if i not in self._down:
                yield exchange

    def __getitem__(self, sequence_number):
        if sequence_number not in self._down:
            return [self.exchanges[sequence_number]]
        return random.sample(self, HANDOFF_NUM)

    def mark_down(self, sequence_number):
        self._down.add(sequence_number)

    def mark_up(self, sequence_number):
        # TODO: add a broadcast-channel listener for node-up messages
        try:
            self._down.remove(sequence_number)
        except KeyError:
            pass
