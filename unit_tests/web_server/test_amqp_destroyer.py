# -*- coding: utf-8 -*-
"""
test_amqp_destroyer.py

test diyapi_web_server/amqp_destroyer.py
"""
import os
import unittest
import uuid
import random
import time

from unit_tests.util import generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from diyapi_web_server.exceptions import DestroyFailedError
from messages.destroy_key import DestroyKey
from messages.destroy_key_reply import DestroyKeyReply

from diyapi_web_server.amqp_destroyer import AMQPDestroyer


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestAMQPDestroyer(unittest.TestCase):
    """test diyapi_web_server/amqp_destroyer.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self.channel = util.MockChannel()
        self.amqp_handler = util.FakeAMQPHandler()
        self.amqp_handler.channel = self.channel
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next
        self._real_sample = random.sample
        random.sample = util.fake_sample

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1
        random.sample = self._real_sample

    def _make_messages(self, avatar_id, timestamp, key):
        base_size = 12345
        messages = []
        messages_to_append = []
        for i in xrange(len(self.exchange_manager)):
            request_id = uuid.UUID(int=i).hex
            message = DestroyKey(
                request_id,
                avatar_id,
                self.amqp_handler.exchange,
                self.amqp_handler.queue_name,
                timestamp,
                key,
                i + 1,   # segment_number
                0        # version number
            )
            reply = DestroyKeyReply(
                request_id,
                DestroyKeyReply.successful,
                base_size + i
            )
            messages.append((message, self.exchange_manager[i]))
            if self.exchange_manager.is_down(i):
                for exchange in self.exchange_manager.handoff_exchanges(i):
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, exchange
                    )].put(reply)
                    messages_to_append.append((message, exchange))
            else:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, self.exchange_manager[i]
                )].put(reply)
        messages.extend(messages_to_append)
        return base_size, messages

    def test_destroy(self):
        """test successful destroy"""
        avatar_id = 1001
        key = self._key_generator.next()
        timestamp = time.time()
        base_size, messages = self._make_messages(avatar_id, timestamp, key)

        destroyer = AMQPDestroyer(self.amqp_handler, self.exchange_manager)
        size_deleted = destroyer.destroy(avatar_id, key, timestamp, 0)

        self.assertEqual(size_deleted, base_size)

        expected = [
            (message.marshall(), exchange)
            for message, exchange in messages
        ]
        actual = [
            (message.marshall(), exchange)
            for message, exchange in self.amqp_handler.messages
        ]
        self.assertEqual(
            actual, expected, 
            'destroyer did not send expected messages %s %s' % (
                len(expected), len(actual),
            )
        )

    def test_destroy_with_failure(self):
        """test failed destroy"""
        avatar_id = 1001
        key = self._key_generator.next()
        timestamp = time.time()
        self.exchange_manager.mark_down(0)
        base_size, messages = self._make_messages(avatar_id, timestamp, key)
        self.exchange_manager.mark_up(0)

        destroyer = AMQPDestroyer(self.amqp_handler, self.exchange_manager)
        timeout = 1.0
        self.assertRaises(
            DestroyFailedError, 
            destroyer.destroy, avatar_id, key, timestamp, timeout
        )

if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
