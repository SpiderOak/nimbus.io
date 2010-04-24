# -*- coding: utf-8 -*-
"""
amqp_exchange_manager.py

A class that keeps track of which exchanges are responding.
"""
import random


HANDOFF_NUM = 2


class AMQPExchangeManager(object):
    def __init__(self, exchanges):
        self.exchanges = list(exchanges)
        self._down = set()

    @property
    def num_exchanges(self):
        return len(self.exchanges)

    def __len__(self):
        return len(self.exchanges) - len(self._down)

    def __iter__(self):
        for i, exchange in enumerate(self.exchanges):
            if i not in self._down:
                yield exchange

    def __getitem__(self, exchange_num):
        if exchange_num not in self._down:
            return [self.exchanges[exchange_num]]
        return random.sample(self, HANDOFF_NUM)

    def is_down(self, exchange_num):
        return exchange_num in self._down

    def mark_down(self, exchange_num):
        self._down.add(exchange_num)

    def mark_up(self, exchange_num):
        # TODO: add a broadcast-channel listener for node-up messages
        try:
            self._down.remove(exchange_num)
        except KeyError:
            pass
