# -*- coding: utf-8 -*-
"""
test_collection_access_control.py

Ticket #43 Implement access_control properties for collections

This tests the logic in using the JSON access_control file
"""
from collections import namedtuple
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from tools.collection_access_control import check_access_control, \
    read_access, \
    write_access, \
    list_access, \
    delete_access, \
    access_allowed, \
    access_requires_password_authentication, \
    access_forbidden

# represetns a WebOb request
_mock_request = namedtuple("MockRequest",
                           ["method", "path", "headers", "remote_addr"])
_default_request = \
    _mock_request(method=None, path=None, headers={}, remote_addr=None)

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
               access_control={"allow_unauth_read_access" : True}, 
               expected_result=access_allowed),
]

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
    unittest.main()

