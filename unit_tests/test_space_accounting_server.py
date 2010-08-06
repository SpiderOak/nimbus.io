# -*- coding: utf-8 -*-
"""
test_space_accounting_server.py

test space accounting
"""
import datetime
import time
import unittest
import uuid

from diyapi_tools.standard_logging import initialize_logging
from messages.space_accounting_detail import SpaceAccountingDetail
from messages.space_usage import SpaceUsage

from diyapi_space_accounting_server.diyapi_space_accounting_server_main import \
    _create_state, _floor_hour, _handle_detail, _handle_space_usage, \
    _flush_to_database
from diyapi_space_accounting_server.space_accounting_database import \
    SpaceAccountingDatabase

_log_path = "/var/log/pandora/test_space_accounting_server.log"
_exchange = "reply-exchange"
_reply_routing_header = "test_space_accounting"

def _detail(state, avatar_id, timestamp, detail_type, detail_bytes):
    message = SpaceAccountingDetail(
        avatar_id, 
        timestamp,
        detail_type,
        detail_bytes
    )
    _handle_detail(state, message.marshall())


class TestSpaceAccountingServer(unittest.TestCase):
    """test message handling in space accounting server"""

    def setUp(self):
        initialize_logging(_log_path)
        self.tearDown()

    def tearDown(self):
        pass

    def test_detail(self):
        """test various forms of SpaceAccountingDetail"""
        avatar_id = 1001
        bytes_added = 42
        timestamp = time.time()
        state = _create_state()

        _detail(
            state, 
            avatar_id,
            timestamp,
            SpaceAccountingDetail.bytes_added,
            bytes_added
        )            

        hour = _floor_hour(datetime.datetime.fromtimestamp(timestamp))
        self.assertEqual(state["data"].has_key(hour), True, state)
        hour_data = state["data"][hour]
        self.assertEqual(hour_data.has_key(avatar_id), True, state)
        avatar_data = hour_data[avatar_id]
        self.assertEqual(avatar_data.has_key("bytes_added"), True, state)
        self.assertEqual(avatar_data["bytes_added"], bytes_added, state)

        _flush_to_database(state, hour)

    def test_usage(self):
        """test SpaceUsage"""
        request_id = uuid.uuid1().hex
        avatar_id = 1001
        total_bytes_added = 42 * 1024 * 1024 * 1000
        total_bytes_removed = 21  * 1024 * 1024 * 50
        total_bytes_retrieved = 66 * 1024 * 1024 * 25
        state = _create_state()

        # clear out any old stats
        space_accounting_database = SpaceAccountingDatabase()
        space_accounting_database.clear_avatar_stats(avatar_id)
        space_accounting_database.commit()

        for _ in xrange(1000):
            _detail(
                state, 
                avatar_id,
                time.time(),
                SpaceAccountingDetail.bytes_added,
                total_bytes_added / 1000
            )            

        for _ in xrange(50):
            _detail(
                state, 
                avatar_id,
                time.time(),
                SpaceAccountingDetail.bytes_removed,
                total_bytes_removed / 50
            )            

        for _ in xrange(25):
            _detail(
                state, 
                avatar_id,
                time.time(),
                SpaceAccountingDetail.bytes_retrieved,
                total_bytes_retrieved / 25
            )            

        hour = _floor_hour(datetime.datetime.now())
        self.assertEqual(state["data"].has_key(hour), True, state)
        _flush_to_database(state, hour)

        space_usage_request = SpaceUsage(
            request_id,
            avatar_id,
            _exchange,
            _reply_routing_header
        )
        marshalled_message = space_usage_request.marshall()

        replies = _handle_space_usage(state, marshalled_message)
        self.assertEqual(len(replies), 1)
        [(reply_exchange, reply_routing_key, reply, ), ] = replies
        self.assertEqual(reply_exchange, _exchange)
        self.assertEqual(
            reply_routing_key, 
            "%s.space_usage_reply" % (_reply_routing_header, )
        )
        self.assertEqual(reply.request_id, request_id)
        self.assertEqual(reply.result, 0, reply.error_message)
        self.assertEqual(
            reply.bytes_added, 
            total_bytes_added, 
            (reply.bytes_added, total_bytes_added, )
        )
        self.assertEqual(
            reply.bytes_removed, 
            total_bytes_removed, 
            (reply.bytes_removed, total_bytes_removed, )
        )
        self.assertEqual(
            reply.bytes_retrieved, 
            total_bytes_retrieved, 
            (reply.bytes_retrieved, total_bytes_retrieved, )
        )

if __name__ == "__main__":
    unittest.main()

