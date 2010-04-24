# -*- coding: utf-8 -*-
"""
amqp_handoff_sender.py

Sends a message to an exchange, handing it off if the exchange is down.
"""
import gevent
from gevent.queue import Queue


NODE_TIMEOUT = 10 # sec


class AMQPHandoffSender(object):
    def __init__(self, amqp_handler, exchange_manager, timeout=NODE_TIMEOUT):
        self.amqp_handler = amqp_handler
        self.exchange_manager = exchange_manager
        self.timeout = timeout

    # TODO: fix this codesmell
    @property
    def reply_exchange(self):
        return self.amqp_handler.exchange

    @property
    def reply_queue(self):
        return self.amqp_handler.queue_name

    def send_to_exchange(self, exchange_num, message, result_queue=None):
        if result_queue is None:
            result_queue = Queue()
        replies = []
        for exchange in self.exchange_manager[exchange_num]:
            reply_queue = self.amqp_handler.send_message(message, exchange)
            reply = gevent.spawn(reply_queue.get)
            replies.append((exchange, reply))
        gevent.joinall([reply for (exchange, reply) in replies], self.timeout)
        down = []
        for exchange, reply in replies:
            if reply.ready():
                result_queue.put(reply.value)
            else:
                # TODO: fix this codesmell
                exchange_num = self.exchange_manager.exchanges.index(exchange)
                self.exchange_manager.mark_down(exchange_num)
                down.append(exchange_num)
        for exchange_num in down:
            # TODO: perhaps doing this recursively is not a good idea
            self.send_to_exchange(exchange_num, message, result_queue)
        return result_queue
