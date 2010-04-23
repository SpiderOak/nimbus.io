# -*- coding: utf-8 -*-
"""
test_util.py

test diyapi_web_server/util.py
"""
import unittest

from diyapi_web_server import util


_valid_usernames = [
    'test',
    '123foo',
    'foo123',
    '123-foo',
    '1-2-3',
]


class FakeReq(object):
    def __init__(self, host):
        self.host = host


class TestGetUsernameFromReq(unittest.TestCase):
    """test diyapi_web_server/util.py"""
    def test_matches_diy(self):
        for username in _valid_usernames:
            req = FakeReq('%s.diy.spideroak.com' % (username,))
            self.assertEqual(username, util.get_username_from_req(req))

    def test_matches_diy_with_port(self):
        for username in _valid_usernames:
            req = FakeReq('%s.diy.spideroak.com:443' % (username,))
            self.assertEqual(username, util.get_username_from_req(req))

    def test_returns_none_for_nonmatch(self):
        for username in _valid_usernames:
            req = FakeReq('localhost')
            self.assertEqual(None, util.get_username_from_req(req))


if __name__ == "__main__":
    unittest.main()
