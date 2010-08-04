# -*- coding: utf-8 -*-
"""
amqp_handler.py

A class that facilitates sending AMQP messages and receiving replies.
"""
import os
import errno
import logging
from collections import defaultdict
from weakref import ref as WeakReference
from weakref import WeakValueDictionary
from socket import error as socket_error

import gevent
from gevent.queue import Queue
from gevent.coros import RLock

import amqplib.client_0_8 as amqp

from diyapi_tools import amqp_connection
from diyapi_tools.message_driven_process import _create_bindings

from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_listmatch_reply import DatabaseListMatchReply
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.destroy_key_reply import DestroyKeyReply
from messages.hinted_handoff_reply import HintedHandoffReply
from messages.stat_reply import StatReply
from messages.space_usage_reply import SpaceUsageReply
from messages.process_status import ProcessStatus


_local_node_name = os.environ['SPIDEROAK_MULTI_NODE_NAME']
_queue_name = 'web-server-%s' % (_local_node_name,)

MESSAGE_TYPES = dict(
    ('%s.%s' % (_queue_name, message_type.routing_tag), message_type)
    for message_type in [
        ArchiveKeyStartReply,
        ArchiveKeyNextReply,
        ArchiveKeyFinalReply,
        DatabaseListMatchReply,
        RetrieveKeyStartReply,
        RetrieveKeyNextReply,
        RetrieveKeyFinalReply,
        DestroyKeyReply,
        HintedHandoffReply,
        StatReply,
        SpaceUsageReply,
    ]
)
MESSAGE_TYPES['process_status'] = ProcessStatus


class AMQPHandler(object):
    def __init__(self):
        self._chan_wait = None
        self.log = logging.getLogger('AMQPHandler')

        self.queue_name = _queue_name
        self.exchange = amqp_connection.local_exchange_name
        self.routing_key_binding = '%s.*' % (self.queue_name,)
        self.reply_queues = WeakValueDictionary()
        self._send_lock = RLock()
        self.subscriptions = defaultdict(list)

    def send_message(self, message, exchange=None):
        if exchange is None:
            exchange = self.exchange

        with self._send_lock:
            try:
                reply_queue = self.reply_queues[message.request_id]
            except AttributeError:
                reply_queue = None
            except KeyError:
                reply_queue = self.reply_queues[message.request_id] = Queue()

            self.channel.basic_publish(
                amqp.Message(message.marshall()),
                exchange=exchange,
                routing_key=message.routing_key,
                mandatory=True
            )

            return reply_queue

    def subscribe(self, message_type, callback):
        self.subscriptions[message_type].append(WeakReference(callback))

    def _handle_subscriptions(self, message_type, message):
        if message_type not in self.subscriptions:
            return False
        handled = False
        subscriptions = self.subscriptions[message_type]
        dead_refs = []
        for i, ref in enumerate(subscriptions):
            callback = ref()
            if callback is None:
                dead_refs.append(i)
                continue
            callback(message)
            handled = True
        dead_refs.reverse()
        for i in dead_refs:
            del subscriptions[i]
        if not subscriptions:
            del self.subscriptions[message_type]
        return handled

    def _callback(self, amqp_message):
        routing_key = amqp_message.delivery_info['routing_key']
        try:
            message_type = MESSAGE_TYPES[routing_key]
        except KeyError:
            self.log.debug('skipping unknown routing key %r' % (routing_key,))
            return
        message = message_type.unmarshall(amqp_message.body)
        handled_reply = False
        try:
            self.reply_queues[message.request_id].put(message)
            handled_reply = True
        except (AttributeError, KeyError):
            pass
        handled_subscrip = self._handle_subscriptions(message_type, message)
        if not (handled_reply or handled_subscrip):
            msg = 'Received unhandled message: %s' % (
                    message.__class__.__name__,)
            try:
                msg += ', request_id=%r' % (
                    message.request_id,)
            except AttributeError:
                pass
            self.log.debug(msg)

    def _run(self):
        self.log.debug('start AMQP loop')
        try:
            while True:
                try:
                    self.channel.wait()
                except (KeyboardInterrupt, SystemExit):
                    self.log.info('KeyboardInterrupt or SystemExit')
                    return
                except socket_error, instance:
                    if instance.errno == errno.EINTR:
                        self.log.warn(
                            'Interrupted system call: '
                            'assuming SIGTERM %s' % (instance,))
                        return
                    else:
                        raise
        except gevent.GreenletExit:
            self.log.info('GreenletExit')
        self.log.debug('end AMQP loop')

        self.channel.basic_cancel(self.amqp_tag)
        self.channel.close()
        self.connection.close()

    def start(self):
        self.connection = amqp_connection.open_connection()
        self.channel = self.connection.channel()
        amqp_connection.create_exchange(self.channel)
        self.log.debug('binding: queue_name=%r, routing_key_binding=%r' % (
            self.queue_name, self.routing_key_binding))
        _create_bindings(
            self.channel,
            self.queue_name,
            True,   # queue_durable
            False,  # queue_auto_delete
            amqp_connection.local_exchange_name,
            self.routing_key_binding
        )

        # Let AMQP know to send us messages
        self.amqp_tag = self.channel.basic_consume(
            queue=self.queue_name,
            no_ack=True,
            callback=self._callback
        )

        self._chan_wait = gevent.spawn(self._run)

    def stop(self):
        if self._chan_wait:
            chan_wait = self._chan_wait
            self._chan_wait = None
            chan_wait.kill(block=True)
