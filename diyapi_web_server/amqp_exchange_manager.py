# -*- coding: utf-8 -*-
"""
amqp_exchange_manager.py

A class that keeps track of which exchanges are responding.
"""
import random


HANDOFF_NUM = 2


class AMQPExchangeManager(list):
    def __init__(self, exchanges):
        super(AMQPExchangeManager, self).__init__(exchanges)
        self._down = set()

    def up(self):
        return [
            exchange
            for i, exchange in enumerate(self)
            if not self.is_down(i)
        ]

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

    def handoff_exchanges(self, exchange_num):
        return random.sample(self.up(), HANDOFF_NUM)
