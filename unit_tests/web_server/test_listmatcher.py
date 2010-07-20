# -*- coding: utf-8 -*-
"""
test_listmatcher.py

test diyapi_web_server/listmatcher.py
"""
import os
import unittest
import uuid

from unit_tests.web_server import util

from diyapi_web_server.amqp_data_reader import AMQPDataReader

from messages.database_listmatch_reply import DatabaseListMatchReply

from diyapi_web_server.listmatcher import Listmatcher


EXCHANGES = os.environ['DIY_NODE_EXCHANGES'].split()
AGREEMENT_LEVEL = 8


class TestListmatcher(unittest.TestCase):
    """test diyapi_web_server/listmatcher.py"""
    def setUp(self):
        self.amqp_handler = util.FakeAMQPHandler()
        self.data_readers = [AMQPDataReader(self.amqp_handler, exchange)
                             for exchange in EXCHANGES]
        self.listmatcher = Listmatcher(self.data_readers, AGREEMENT_LEVEL)
        self._real_uuid1 = uuid.uuid1
        uuid.uuid1 = util.fake_uuid_gen().next

    def tearDown(self):
        uuid.uuid1 = self._real_uuid1

    def test_listmatch(self):
        avatar_id = 1001
        prefix = 'a_prefix'
        key_list = ['%s-%d' % (prefix, i) for i in xrange(10)]
        for i, data_reader in enumerate(self.data_readers):
            request_id = uuid.UUID(int=i).hex
            reply = DatabaseListMatchReply(
                request_id,
                DatabaseListMatchReply.successful,
                key_list=key_list
            )
            self.amqp_handler.replies_to_send_by_exchange[(
                request_id, data_reader.exchange
            )].put(reply)

        result = self.listmatcher.listmatch(avatar_id, prefix, 0)
        self.assertEqual(result, key_list)


if __name__ == "__main__":
    from diyapi_tools.standard_logging import initialize_logging
    _log_path = "/var/log/pandora/test_web_server.log"
    initialize_logging(_log_path)
    unittest.main()
