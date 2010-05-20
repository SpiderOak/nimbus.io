# -*- coding: utf-8 -*-
"""
amqp_space_accounter.py

Sends space accounting messages.
"""
from messages.space_accounting_detail import SpaceAccountingDetail


class AMQPSpaceAccounter(object):
    def __init__(self, amqp_handler, exchange):
        self.amqp_handler = amqp_handler
        self.exchange = exchange

    def added(self, avatar_id, timestamp, bytes):
        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_added,
            bytes
        )
        self.amqp_handler.send_message(message, self.exchange)

    def retrieved(self, avatar_id, timestamp, bytes):
        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_retrieved,
            bytes
        )
        self.amqp_handler.send_message(message, self.exchange)

    def removed(self, avatar_id, timestamp, bytes):
        message = SpaceAccountingDetail(
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_removed,
            bytes
        )
        self.amqp_handler.send_message(message, self.exchange)
