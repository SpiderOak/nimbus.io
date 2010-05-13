# -*- coding: utf-8 -*-
"""
test_application.py

test diyapi_web_server/application.py
"""
import os
import unittest
import uuid
import random
import time
import zlib
import hashlib

from webtest import TestApp

from unit_tests.util import random_string, generate_key
from unit_tests.web_server import util
from diyapi_web_server.zfec_segmenter import ZfecSegmenter
from diyapi_web_server.amqp_exchange_manager import AMQPExchangeManager
from messages.archive_key_start_reply import ArchiveKeyStartReply
from messages.archive_key_next_reply import ArchiveKeyNextReply
from messages.archive_key_final_reply import ArchiveKeyFinalReply
from messages.database_listmatch_reply import DatabaseListMatchReply
from messages.retrieve_key_start_reply import RetrieveKeyStartReply
from messages.retrieve_key_next_reply import RetrieveKeyNextReply
from messages.retrieve_key_final_reply import RetrieveKeyFinalReply
from messages.destroy_key_reply import DestroyKeyReply

from diyapi_web_server import application
from diyapi_web_server.application import Application


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()


class TestApplication(unittest.TestCase):
    """test diyapi_web_server/application.py"""
    def setUp(self):
        self.exchange_manager = AMQPExchangeManager(EXCHANGES)
        self.authenticator = util.FakeAuthenticator(0)
        self.amqp_handler = util.FakeAMQPHandler()
        self.app = TestApp(Application(
            self.amqp_handler,
            self.exchange_manager,
            self.authenticator
        ))
        self._key_generator = generate_key()
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next
        self._real_sample = random.sample
        random.sample = util.fake_sample
        self._real_timeout = application.EXCHANGE_TIMEOUT
        application.EXCHANGE_TIMEOUT = 0

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1
        random.sample = self._real_sample
        application.EXCHANGE_TIMEOUT = self._real_timeout

    def test_unauthorized(self):
        self.app.app.authenticator = util.FakeAuthenticator(None)
        self.app.get(
            '/data/some-key',
            dict(action='listmatch'),
            status=401
        )

    def test_archive_small(self):
        for i in xrange(self.exchange_manager.num_exchanges):
            request_id = uuid.UUID(int=i).hex
            self.amqp_handler.replies_to_send[request_id].put(
                ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                )
            )
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        resp = self.app.post('/data/' + key, content)
        self.assertEqual(resp.body, 'OK')

    def test_archive_small_with_handoff(self):
        self.exchange_manager.mark_down(0)
        for i in xrange(self.exchange_manager.num_exchanges):
            request_id = uuid.UUID(int=i).hex
            message = ArchiveKeyFinalReply(
                request_id,
                ArchiveKeyFinalReply.successful,
                0
            )
            for exchange in self.exchange_manager[i]:
                self.amqp_handler.replies_to_send_by_exchange[(
                    request_id, exchange
                )].put(message)
        self.exchange_manager.mark_up(0)
        content = random_string(64 * 1024)
        key = self._key_generator.next()
        resp = self.app.post('/data/' + key, content)
        self.assertEqual(resp.body, 'OK')

    def test_archive_large(self):
        for i in xrange(self.exchange_manager.num_exchanges):
            request_id = uuid.UUID(int=i).hex
            self.amqp_handler.replies_to_send[request_id].put(
                ArchiveKeyStartReply(
                    request_id,
                    ArchiveKeyStartReply.successful,
                    0
                )
            )
            self.amqp_handler.replies_to_send[request_id].put(
                ArchiveKeyNextReply(
                    request_id,
                    ArchiveKeyNextReply.successful,
                    0
                )
            )
            self.amqp_handler.replies_to_send[request_id].put(
                ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                )
            )
        key = self._key_generator.next()
        with open('/dev/urandom', 'rb') as f:
            resp = self.app.request(
                '/data/' + key,
                method='POST',
                headers={
                    'content-length': str(1024 * 1024 * 3), # 3mb
                },
                body_file=f,
            )
        self.assertEqual(resp.body, 'OK')

    def test_archive_large_with_handoff(self):
        self.exchange_manager.mark_down(0)
        for i in xrange(self.exchange_manager.num_exchanges):
            request_id = uuid.UUID(int=i).hex
            for message in [
                ArchiveKeyStartReply(
                    request_id,
                    ArchiveKeyStartReply.successful,
                    0
                ),
                ArchiveKeyNextReply(
                    request_id,
                    ArchiveKeyNextReply.successful,
                    0
                ),
                ArchiveKeyFinalReply(
                    request_id,
                    ArchiveKeyFinalReply.successful,
                    0
                ),
            ]:
                for exchange in self.exchange_manager[i]:
                    self.amqp_handler.replies_to_send_by_exchange[(
                        request_id, exchange
                    )].put(message)
        self.exchange_manager.mark_up(0)
        key = self._key_generator.next()
        with open('/dev/urandom', 'rb') as f:
            resp = self.app.request(
                '/data/' + key,
                method='POST',
                headers={
                    'content-length': str(1024 * 1024 * 3), # 3mb
                },
                body_file=f,
            )
        self.assertEqual(resp.body, 'OK')


    # TODO: test archive without content-length header

    def test_listmatch(self):
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        request_id = uuid.UUID(int=0).hex
        self.amqp_handler.replies_to_send[request_id].put(
            DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=key_list
            )
        )
        resp = self.app.get('/data/%s' % (prefix,), dict(action='listmatch'))
        self.assertEqual(resp.body, repr(key_list))

    def test_retrieve_small(self):
        key = self._key_generator.next()
        timestamp = time.time()
        file_size = 1024
        data_content = random_string(file_size)
        file_adler32 = zlib.adler32(data_content)
        file_md5 = hashlib.md5(data_content).digest()

        segmenter = ZfecSegmenter(
            8, # TODO: min_segments
            self.exchange_manager.num_exchanges)
        segments = segmenter.encode(data_content)

        for segment_number, segment in enumerate(segments):
            segment_number += 1
            segment_adler32 = zlib.adler32(segment)
            segment_md5 = hashlib.md5(segment).digest()
            request_id = uuid.UUID(int=segment_number - 1).hex
            self.amqp_handler.replies_to_send[request_id].put(
                RetrieveKeyStartReply(
                    request_id,
                    RetrieveKeyStartReply.successful,
                    timestamp,
                    False,
                    0,  # version number
                    segment_number,
                    1,  # num slices
                    len(data_content),
                    file_size,
                    file_adler32,
                    file_md5,
                    segment_adler32,
                    segment_md5,
                    segment
                )
            )

        resp = self.app.get('/data/%s' % (key,))
        self.assertEqual(len(resp.body), file_size)
        self.assertEqual(resp.body, data_content)

    def test_retrieve_large(self):
        key = self._key_generator.next()
        timestamp = time.time()
        n_slices = 3
        slice_size = 1
        file_size = slice_size * n_slices
        data_list = [random_string(slice_size)
                     for _ in xrange(n_slices)]
        data_content = ''.join(data_list)
        file_adler32 = zlib.adler32(data_content)
        file_md5 = hashlib.md5(data_content).digest()

        for sequence_number, slice in enumerate(data_list):
            segmenter = ZfecSegmenter(
                8, # TODO: min_segments
                self.exchange_manager.num_exchanges)
            segments = segmenter.encode(slice)
            for segment_number, segment in enumerate(segments):
                segment_number += 1
                segment_adler32 = zlib.adler32(segment)
                segment_md5 = hashlib.md5(segment).digest()
                request_id = uuid.UUID(int=segment_number - 1).hex
                if sequence_number == 0:
                    self.amqp_handler.replies_to_send[request_id].put(
                        RetrieveKeyStartReply(
                            request_id,
                            RetrieveKeyStartReply.successful,
                            timestamp,
                            False,
                            0,  # version number
                            segment_number,
                            n_slices,
                            len(slice),
                            file_size,
                            file_adler32,
                            file_md5,
                            segment_adler32,
                            segment_md5,
                            segment
                        )
                    )
                elif sequence_number == n_slices - 1:
                    self.amqp_handler.replies_to_send[request_id].put(
                        RetrieveKeyFinalReply(
                            request_id,
                            sequence_number,
                            RetrieveKeyFinalReply.successful,
                            segment
                        )
                    )
                else:
                    self.amqp_handler.replies_to_send[request_id].put(
                        RetrieveKeyNextReply(
                            request_id,
                            sequence_number,
                            RetrieveKeyNextReply.successful,
                            segment
                        )
                    )

        resp = self.app.get('/data/%s' % (key,))
        self.assertEqual(len(resp.body), file_size)
        self.assertEqual(resp.body, data_content)

    def test_destroy(self):
        key = self._key_generator.next()
        base_size = 12345
        timestamp = time.time()
        for i, exchange in enumerate(self.exchange_manager):
            request_id = uuid.UUID(int=i).hex
            self.amqp_handler.replies_to_send[request_id].put(
                DestroyKeyReply(
                    request_id,
                    DestroyKeyReply.successful,
                    base_size + i
                )
            )

        resp = self.app.delete('/data/%s' % (key,))
        self.assertEqual(resp.body, 'OK')
        # TODO: check for space accounting message


if __name__ == "__main__":
    unittest.main()
