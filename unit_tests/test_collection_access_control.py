# -*- coding: utf-8 -*-
"""
test_collection_access_control.py

Ticket #43 Implement access_control properties for collections

This tests the logic in using the JSON access_control file
"""
import json
import logging
from collections import namedtuple
import re
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.collection_access_control import version, \
    cleanse_access_control, \
    check_access_control, \
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

_cleanse_test_case = namedtuple("CleanseTestCase",
                                ["raw_data", "expected_dict", "error_re"])

_cleanse_test_cases = [
    _cleanse_test_case(raw_data=None,
                       expected_dict=None,
                       error_re=None),
    _cleanse_test_case(raw_data='a' * (16 * 1024 + 1),
                       expected_dict=None,
                       error_re=[re.compile("^.*too large.*$"), ]),
    _cleanse_test_case(raw_data="[xxx",
                       expected_dict=None,
                       error_re=[re.compile("^.*Unable to parse.*$"), ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0", 
                                            "allow_unauth_read" : "pork"}),
                       expected_dict=None,
                       error_re=[re.compile("^.*Expected bool.*$"), ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "allow_unauth_read" : True}),
                       expected_dict={version : "1.0",
                                      allow_unauth_read : True},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "ipv4_whitelist" : None}),
                       expected_dict={version : "1.0",
                                      ipv4_whitelist : None},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "ipv4_whitelist" : "clam"}),
                       expected_dict=None,
                       error_re=[re.compile(
                            "^.*invalid ipv4_whitelist type.*$"), ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "ipv4_whitelist" : []}),
                       expected_dict={version : "1.0",
                                      ipv4_whitelist : []},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "ipv4_whitelist" : [42]}),
                       expected_dict=None,
                       error_re=[re.compile(
                            "^.*invalid ipv4_whitelist entry type.*$"), ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "ipv4_whitelist" : ["clam"]}),
                       expected_dict=None,
                       error_re=[re.compile(
                            "^.*invalid ipv4_whitelist address.*$"), ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "ipv4_whitelist" : \
                                                ["192.168.1.102"]}),
                       expected_dict={version : "1.0",
                                      ipv4_whitelist : ["192.168.1.102/32"]},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "unauth_referrer_whitelist" : \
                                                None}),
                       expected_dict={version : "1.0",
                                      unauth_referrer_whitelist : None},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "unauth_referrer_whitelist" : \
                                                "clam"}),
                       expected_dict=None,
                       error_re=[re.compile(
                            "^.*invalid unauth_referrer_whitelist type.*$") ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "unauth_referrer_whitelist" : []}),
                       expected_dict={version : "1.0",
                                      unauth_referrer_whitelist : []},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "unauth_referrer_whitelist" : \
                                                [42]}),
                       expected_dict=None,
                       error_re=[re.compile(
                            "^.*invalid unauth_referrer_whitelist entry type.*$"), ]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "unauth_referrer_whitelist" : \
                                                ["example.com/myapp"]}),
                       expected_dict={version : "1.0",
                                      unauth_referrer_whitelist : \
                                      ["example.com/myapp"]},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "locations" : None}),
                       expected_dict={version : "1.0",
                                      locations : None},
                       error_re=None),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "locations" : "clam"}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*invalid locations type.*$")]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "locations" : ["clam"]}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*invalid locations entry type.*$")]),

    # JSON converts 42 to string
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "locations" : [{42 : "clam"}]}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*invalid locations entry key.*$")]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "0.0",
                                            "locations" : [{"prefix" : 42}]}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*invalid locations prefix type.*$")]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "locations" : [{"regexp" : "["}]}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*invalid locations regexp.*$")]),
    _cleanse_test_case(raw_data=json.dumps({"version" : "1.0",
                                            "locations" : \
                                                [{"prefix" : "aaa",
                                                  locations : []}]}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*invalid locations entry key.*$")]),
    _cleanse_test_case(raw_data=json.dumps(
        {"version" : "1.0",
         "locations" : [{"prefix" : "aaa",
                         "allow_unauth_read" : "pork"}]}),
                       expected_dict=None,
                       error_re=[re.compile(
                        "^.*Expected bool.*$")]),
    _cleanse_test_case(raw_data=json.dumps(
        {"version" : "1.0",
         "locations" : [{"prefix" : "aaa",
                         "allow_unauth_read" : False}]}),
                       expected_dict={version : "1.0",
                                      locations : [
                                        {"prefix" : "aaa",
                                         allow_unauth_read : False}]},
                       error_re=None),
]

# represents a WebOb request`
_mock_request = namedtuple("MockRequest",
                           ["method", "url", "headers", "remote_addr"])
_default_request = \
    _mock_request(method=None, url=None, headers={}, remote_addr=None)

_check_test_case = namedtuple("CheckTestCase", 
                              ["access_type", 
                               "request", 
                               "access_control", 
                               "expected_result"])

_check_test_cases = [
    # test no access_control
    _check_test_case(access_type=read_access,
               request=_default_request,  
               access_control={}, 
               expected_result=access_requires_password_authentication),

    # test read access with allow_unauth_read set
    _check_test_case(access_type=read_access,
               request=_default_request,  
               access_control={version : "1.0", 
                               allow_unauth_read : True}, 
               expected_result=access_allowed),

    # test write access with allow_unauth_write set
    _check_test_case(access_type=write_access,
               request=_default_request,  
               access_control={version : "1.0",
                               allow_unauth_write : True}, 
               expected_result=access_allowed),

    # test list access with allow_unauth_list set
    _check_test_case(access_type=list_access,
               request=_default_request,  
               access_control={version : "1.0",
                               allow_unauth_list : True}, 
               expected_result=access_allowed),

    # test delete access with allow_unauth_delete set
    _check_test_case(access_type=delete_access,
               request=_default_request,  
               access_control={version : "1.0",
                               allow_unauth_delete : True}, 
               expected_result=access_allowed),

    # test a single address ipv4 whitelist
    # 1) remote_addr in the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={version : "1.0",
                               ipv4_whitelist : ["192.168.1.200"]}, 
               expected_result=access_requires_password_authentication),

    # test a single address ipv4 whitelist
    # 1) remote_addr outside the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={version : "1.0", 
                               ipv4_whitelist : ["192.168.1.201"]}, 
               expected_result=access_forbidden),

    # test an address range ipv4 whitelist
    # 1) remote_addr in the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={version : "1.0",
                               ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_requires_password_authentication),

    # test an address range ipv4 whitelist
    # 2) remote_addr outside the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.2.200"),  
               access_control={version : 1.0,
                               ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_forbidden),

    # test an address range ipv4 whitelist with allow_unauth_read
    # 1) remote_addr in the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.1.200"),  
               access_control={version : "1.0",
                               allow_unauth_read : True, 
                               ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_allowed),

    # test an address range ipv4 whitelist with allow_unauth_read
    # 2) remote_addr outside the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(remote_addr="192.168.2.200"),  
               access_control={version : "1.0",
                               allow_unauth_read : True,
                               ipv4_whitelist : ["192.168.1.0/24"]}, 
               expected_result=access_forbidden),

    # test an unauth_referrer whitelist, with no 'Referer' header
    _check_test_case(access_type=read_access,
               request=_default_request,
               access_control={version : "1.0",
                               unauth_referrer_whitelist : \
                                ["example.com/myapp"]}, 
               expected_result=access_forbidden),

    # test an unauth_referrer whitelist with a valid 'Referer' header 
    _check_test_case(access_type=read_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/myapp/login"}),  
               access_control={version : "1.0",
                               unauth_referrer_whitelist : \
                                ["example.com/myapp"]}, 
               expected_result=access_requires_password_authentication),

    # test an unauth_referrer whitelist with an invalid 'Referer' header 
    _check_test_case(access_type=read_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/other/login"}),  
               access_control={version : "1.0",
                               unauth_referrer_whitelist : \
                                ["example.com/myapp"]}, 
               expected_result=access_forbidden),

    # test an unauth_referrer whitelist with a valid 'Referer' header 
    # with allow_unauth_write
    _check_test_case(access_type=write_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/myapp/login"}),  
               access_control={version : "1.0",
                               allow_unauth_write : True,
                               unauth_referrer_whitelist : \
                                ["example.com/myapp"]}, 
               expected_result=access_allowed),

    # test an unauth_referrer whitelist with an invalid 'Referer' header 
    # with allow_unauth_write
    _check_test_case(access_type=write_access,
               request=_default_request._replace(
                    headers={"Referer" : "http://example.com/other/login"}),  
               access_control={version : "1.0",
                               allow_unauth_write : True,
                               unauth_referrer_whitelist : \
                                ["example.com/myapp"]}, 
               expected_result=access_forbidden),

    # test 'locations' override reducing permissions
    # test an unauth_referrer whitelist with a valid 'Referer' header 
    # with allow_unauth_write
    _check_test_case(access_type=write_access,
               request=_default_request._replace(
                    url="http://example.com/private/index.html",
                    headers={"Referer" : "http://example.com/myapp/login"}),  
               access_control={version : "1.0",
                               allow_unauth_write : True,
                               unauth_referrer_whitelist : ["example.com"], 
                               locations : [{"prefix" : "private",
                                             allow_unauth_write : False,
                                             unauth_referrer_whitelist : []}]}, 
               expected_result=access_forbidden),

    # test 'locations' override increasing permissions
    # test an address range ipv4 whitelist with allow_unauth_read
    # 2) remote_addr outside the range
    _check_test_case(access_type=read_access,
               request=_default_request._replace(
                    url="http://example.com/public/index.html",
                    remote_addr="192.168.2.200"),  
               access_control={version : "1.0",
                               allow_unauth_read : True,
                               ipv4_whitelist : ["192.168.1.0/24"],
                               locations : [{"regexp" : "^public/.*$",
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
    def test_cleanse_access_control(self):
        """
        test producing a safe access_control dict from possibly malicious data
        """
        for index, test_case in enumerate(_cleanse_test_cases):
            access_control, error_messages = \
                cleanse_access_control(test_case.raw_data)
            if access_control is None:
                access_control_dict = None
            else:
                access_control_dict = json.loads(access_control)
            self.assertEqual(access_control_dict, 
                             test_case.expected_dict,
                             "Test #{0} expected {1} received {2}".format(
                                index+1, 
                                test_case.expected_dict, 
                                access_control_dict))
            if test_case.error_re is None:
                self.assertTrue(error_messages is None,
                                "Test #{0} unexpected error {1}".format(
                                    index+1,
                                    error_messages))
            else:
                self.assertTrue(error_messages is not None,
                                "Test #{0} expect errors {0} got None".format(
                                    index+1,
                                    test_case.error_re))
                match_count = 0
                for error_re in test_case.error_re:
                    for error_message in error_messages:
                        match_object = error_re.match(error_message)
                        if match_object is not None:
                            match_count += 1
                self.assertEqual(match_count, len(test_case.error_re), 
                             "Test #{0} matched {1} expected {2} {3} {4}".format(
                                index+1, 
                                match_count, 
                                len(test_case.error_re),
                                [r.pattern for r in test_case.error_re],
                                str(error_messages)))

    def test_check_access_control(self):
        """
        test various cases of using access control
        """
        for index, test_case in enumerate(_check_test_cases):
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

