# -*- coding: utf-8 -*-
"""
test_collection_access_control.py

Ticket #43 Implement access_control properties for collections

This tests the logic in using the JSON access_control file
"""
import logging
from collections import namedtuple
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.collection_access_control import check_access_control, \
    read_access, \
    write_access, \
    list_access, \
    delete_access, \
    allow_unauth_read, \
    allow_unauth_write, \
    allow_unauth_list, \
    allow_unauth_delete, \
    ipv4_whitelist, \
    unauth_referrer_whitelist, \
    locations, \
    access_allowed, \
    access_requires_password_authentication, \
    access_forbidden

# represents a WebOb request
_mock_request = namedtuple("MockRequest",
                           ["method", "url", "headers", "remote_addr"])
_default_request = \
    _mock_request(method=None, url=None, headers={}, remote_addr=None)

_test_case = namedtuple("TestCase", 
                        ["access_type", 
                         "request", 
                         "access_control", 
                         "expected_result"])

_test_cases = [
    # test no access_control
    _test_case(access_type=read_access,
               request=_default_request,  
               access_control={}, 
               expected_result=access_requires_password_authentication),

    # test read access with allow_unauth_read set
    _test_case(access_type=read_access,
               request=_default_request,  
               access_control={allow_unauth_read : True}, 
               expected_result=access_allowed),

    # test write access with allow_unauth_write set
    _test_case(access_type=write_access,
               request=_default_request,  
               access_control={allow_unauth_write : True}, 
               expected_result=access_allowed),

    # test list access with allow_unauth_list set
    _test_case(access_type=list_access,
               request=_default_request,  
               access_control={allow_unauth_list : True}, 
               expected_result=access_allowed),

    # test delete access with allow_unauth_delete set
    _test_case(access_type=delete_access,
               request=_default_request,  
               access_control={allow_unauth_delete : True}, 
               expected_result=access_allowed),

    # test a single address ipv4 whitelist
    # 1) remote_addr in the range
    _test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={ipv4_whitelist : ["192.168.1.200"]}, 
               expected_result=access_requires_password_authentication),

    # test a single address ipv4 whitelist
    # 1) remote_addr outside the range
    _test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={ipv4_whitelist : ["192.168.1.201"]}, 
               expected_result=access_forbidden),

    # test an address range ipv4 whitelist
    # 1) remote_addr in the range
    _test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_requires_password_authentication),

    # test an address range ipv4 whitelist
    # 2) remote_addr outside the range
    _test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.2.200"),  
               access_control={ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_forbidden),

    # test an address range ipv4 whitelist with allow_unauth_read
    # 1) remote_addr in the range
    _test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={allow_unauth_read : True, 
                               ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_allowed),

    # test an address range ipv4 whitelist with allow_unauth_read
    # 2) remote_addr outside the range
    _test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.2.200"),  
               access_control={allow_unauth_read : True,
                               ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_forbidden),

    # test an unauth_referrer whitelist, with no 'Referer' header
    _test_case(access_type=read_access,
               request=_default_request,
               access_control={
                    unauth_referrer_whitelist : ["example.com/myapp"]}, 
               expected_result=access_forbidden),

    # test an unauth_referrer whitelist with a valid 'Referer' header 
    _test_case(access_type=read_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/myapp/login"}),  
               access_control={
                    unauth_referrer_whitelist : ["example.com/myapp"]}, 
               expected_result=access_requires_password_authentication),

    # test an unauth_referrer whitelist with an invalid 'Referer' header 
    _test_case(access_type=read_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/other/login"}),  
               access_control={
                    unauth_referrer_whitelist : ["example.com/myapp"]}, 
               expected_result=access_forbidden),

    # test an unauth_referrer whitelist with a valid 'Referer' header 
    # with allow_unauth_write
    _test_case(access_type=write_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/myapp/login"}),  
               access_control={
                    allow_unauth_write : True,
                    unauth_referrer_whitelist : ["example.com/myapp"]}, 
               expected_result=access_allowed),

    # test an unauth_referrer whitelist with an invalid 'Referer' header 
    # with allow_unauth_write
    _test_case(access_type=write_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/other/login"}),  
               access_control={
                    allow_unauth_write : True,
                    unauth_referrer_whitelist : ["example.com/myapp"]}, 
               expected_result=access_forbidden),

    # test 'locations' override reducing permissions
    # test an unauth_referrer whitelist with a valid 'Referer' header 
    # with allow_unauth_write
    _test_case(access_type=write_access,
               request=_default_request._replace(
                    url="http://example.com/private/index.html",
                    headers={"Referer" : "http://example.com/myapp/login"}),  
               access_control={
                    allow_unauth_write : True,
                    unauth_referrer_whitelist : ["example.com"], 
                    locations : [{
                        "prefix" : "private",
                        allow_unauth_write : False,
                        unauth_referrer_whitelist : []}]}, 
               expected_result=access_forbidden),

    # test 'locations' override increasing permissions
    # test an address range ipv4 whitelist with allow_unauth_read
    # 2) remote_addr outside the range
    _test_case(access_type=read_access,
               request=_default_request._replace(
                    url="http://example.com/public/index.html",
                    remote_addr="192.168.2.200"),  
               access_control={
                    allow_unauth_read : True,
                    ipv4_whitelist : ["192.168.1.0/24"],
                    locations : [{
                        "regexp" : "^public/.*$",
                        allow_unauth_write : True,
                        ipv4_whitelist : []}]}, 
               expected_result=access_allowed),

]

def _initialize_logging_to_stderr():
    from tools.standard_logging import _log_format_template
    log_level = logging.DEBUG
    handler = logging.StreamHandler(stream=sys.stderr)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

class TestCollectionAccessControl(unittest.TestCase):
    """
    Ticket #43 Implement access_control properties for collections
    This tests the logic in using the JSON access_control file
    """
    def test_cases(self):
        for index, test_case in enumerate(_test_cases):
            result = check_access_control(test_case.access_type, 
                                          test_case.request,
                                          test_case.access_control)
            self.assertEqual(result, 
                             test_case.expected_result, 
                             "Test #{0} expected {1} received {2}".format(
                                index+1, test_case.expected_result, result))

if __name__ == "__main__":
    _initialize_logging_to_stderr()
    unittest.main()

