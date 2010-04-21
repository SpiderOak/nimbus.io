# -*- coding: utf-8 -*-
"""
test_application.py

test diyapi_web_server/application.py
"""
import os
import unittest
import uuid
import time
import zlib
import hashlib

from webtest import TestApp

from zfec.easyfec import Encoder

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_listmatch_reply import DatabaseListMatchReply
from messages.database_key_list_reply import DatabaseKeyListReply
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from diyapi_database_server import database_content
from messages.database_key_destroy_reply import DatabaseKeyDestroyReply

from diyapi_web_server.application import Application


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestApplication(unittest.TestCase):
    """test diyapi_web_server/application.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(
            EXCHANGES, len(EXCHANGES) - 2)
        self.channel = util.MockChannel()
        self.handler = util.FakeAMQPHandler()
        self.handler.channel = self.channel
        self.app = TestApp(Application(self.handler, self.exchange_manager))
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_archive(self):
        for i in xrange(self.exchange_manager.num_exchanges):
            request_id = uuid.UUID(int=i).hex
            self.handler.replies_to_send[request_id] = [
                ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                )
            ]
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        resp = self.app.post('/data/' + key, content)
        self.assertEqual(resp.body, 'OK')

    def test_listmatch(self):
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        request_id = uuid.UUID(int=0).hex
        self.handler.replies_to_send[request_id] = [
            DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=key_list
            )
        ]
        resp = self.app.get('/data/%s' % (prefix,), dict(action='listmatch'))
        self.assertEqual(resp.body, repr(key_list))

    def test_retrieve(self):
        key = self._key_generator.next()
        timestamp = time.time()
        data_content = random_string(64 * 1024)
        file_adler32 = zlib.adler32(data_content)
        file_md5 = hashlib.md5(data_content).digest()
        segment_adler32 = zlib.adler32(data_content)
        segment_md5 = hashlib.md5(data_content).digest()

        encoder = Encoder(self.exchange_manager.min_exchanges,
                          self.exchange_manager.num_exchanges)
        segments = encoder.encode(data_content)

        # TODO: extract helper methods
        for segment_number, segment in enumerate(segments):
            content = database_content.create_content(
                database_content._current_format_version,
                False,
                timestamp,
                0,
                segment_number,
                self.exchange_manager.num_exchanges,
                len(segment),
                len(data_content),
                file_adler32,
                file_md5,
                segment_adler32,
                segment_md5,
                0,
                0,
                0,
                key
            )
            segments[segment_number] = (segment, content)
            request_id = uuid.UUID(int=segment_number).hex
            self.handler.replies_to_send[request_id] = [
                DatabaseKeyListReply(
                    request_id,
                    DatabaseKeyListReply.successful,
                    [content]
                )
            ]

        for i, (segment, content) in enumerate(segments):
            request_id = uuid.UUID(int=segment_number + i + 1).hex
            self.handler.replies_to_send[request_id] = [
                RetrieveKeyStartReply(
                    request_id,
                    RetrieveKeyStartReply.successful,
                    content.timestamp,
                    content.is_tombstone,
                    content.version_number,
                    content.segment_number,
                    content.segment_count,
                    content.segment_size,
                    content.total_size,
                    content.file_adler32,
                    content.file_md5,
                    content.segment_adler32,
                    content.segment_md5,
                    segment
                )
            ]

        resp = self.app.get('/data/%s' % (key,))
        self.assertEqual(resp.body, data_content)

    def test_destroy(self):
        key = self._key_generator.next()
        base_size = 12345
        # TODO: how are we supposed to handle timestamp here?
        timestamp = time.time()
        for i, exchange in enumerate(self.exchange_manager):
            request_id = uuid.UUID(int=i).hex
            self.handler.replies_to_send[request_id] = [
                DatabaseKeyDestroyReply(
                    request_id,
                    DatabaseKeyDestroyReply.successful,
                    base_size + i
                )
            ]

        resp = self.app.delete('/data/%s' % (key,))
        self.assertEqual(resp.body, 'OK')
        # TODO: check for space accounting message


if __name__ == "__main__":
    unittest.main()
