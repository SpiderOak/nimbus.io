# -*- coding: utf-8 -*-
"""
test_amqp_data_archiver.py

test diyapi_web_server/amqp_data_archiver.py
"""
import unittest
import time
import hashlib
import zlib

from unit_tests.util import random_string, generate_key

from unit_tests.web_server.test_amqp_handler import MockChannel
from diyapi_web_server.amqp_handler import AMQPHandler
from messages.archive_key_entire import ArchiveKeyEntire
from messages.archive_key_final_reply import ArchiveKeyFinalReply

from diyapi_web_server.amqp_data_archiver import AMQPDataArchiver


class FakeAMQPHandler(AMQPHandler):
    def send_message(self, message):
        replies = super(FakeAMQPHandler, self).send_message(message)
        self._reply_to_send.request_id = message.request_id
        replies.put(self._reply_to_send)
        return replies


class TestAMQPDataArchiver(unittest.TestCase):
    """test diyapi_web_server/amqp_data_archiver.py"""
    def setUp(self):
        self.channel = MockChannel()
        self.handler = FakeAMQPHandler()
        self.handler.channel = self.channel
        self._key_generator = generate_key()

    def test_archive_entire(self):
        archiver = AMQPDataArchiver(self.handler)
        avatar_id = 1001
        key = self._key_generator.next()
        content = random_string(64 * 1024)
        adler32 = zlib.adler32(content)
        md5 = hashlib.md5(content).digest()
        timestamp = time.time()
        self.handler._reply_to_send = ArchiveKeyFinalReply(
            'request_id (replaced by FakeAMQPHandler)',
            ArchiveKeyFinalReply.successful,
            0)

        prev_size = archiver.archive_entire(avatar_id, key, content, timestamp)

        self.assertEqual(prev_size, 0)
        self.assertEqual(len(self.channel.messages), 1)

        ((amqp_message,), message_args) = self.channel.messages[0]
        self.assertEqual(message_args, dict(
            exchange=self.handler.exchange,
            routing_key=ArchiveKeyEntire.routing_key,
            mandatory=True))

        message = ArchiveKeyEntire.unmarshall(amqp_message.body)
        self.assertEqual(message.avatar_id, avatar_id)
        self.assertEqual(message.key, key)
        self.assertEqual(message.timestamp, timestamp)
        self.assertEqual(message.content, content)
        self.assertEqual(message.md5, md5)
        self.assertEqual(message.adler32, adler32)


if __name__ == "__main__":
    unittest.main()
