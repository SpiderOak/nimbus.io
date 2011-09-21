# -*- coding: utf-8 -*-
"""
test_url_discriminator.py

test recognizing the action required by URLs in the web server
"""
import os
import unittest

from tools.standard_logging import initialize_logging
from web_server.url_discriminator import parse_url, \
        action_list_collections, \
        action_create_collection, \
        action_delete_collection, \
        action_space_usage, \
        action_archive_key, \
        action_list_keys, \
        action_retrieve_key, \
        action_delete_key, \
        action_stat_key

_log_path = "%s/test_url_discriminator.log" % (
    os.environ["NIMBUSIO_LOG_DIR"], 
)

_valid_urls_with_actions = [
    (
        "GET", 
        "http://nimbus.io:8088/customers/test-user-name/collections",
        action_list_collections
    ),
    (
        "GET", 
        "https://nimbus.io/customers/test-user-name/collections",
        action_list_collections
    ),
    (
        "POST", 
        "https://nimbus.io/customers/test-user-name/collections?action=create&name=test-collection-name",
        action_create_collection
    ),
    (
        "DELETE", 
        "https://nimbus.io/customers/test-user-name/collections/test-collection-name",
        action_delete_collection
    ),
    (
        "POST", 
        "https://nimbus.io/customers/test-user-name/collections/test-collection-name?action=delete",
        action_delete_collection
    ),
    (
        "GET", 
        "https://nimbus.io/customers/test-user-name/collections/test-collection-name?action=space_usage",
        action_space_usage
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_archive_key
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/",
        action_list_keys
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_retrieve_key
    ),
    (
        "DELETE", 
        "https://test-collection-name.nimbus.io/data/test-key",
        action_delete_key
    ),
    (
        "POST", 
        "https://test-collection-name.nimbus.io/data/test-key?action=delete",
        action_delete_key
    ),
    (
        "GET", 
        "https://test-collection-name.nimbus.io/data/test-key?action=stat",
        action_stat_key
    ),
]

def _valid_list_collections(match_object):
    return match_object.group("username") == "test-user-name"

def _valid_create_collection(match_object):
    return match_object.group("username") == "test-user-name" \
    and match_object.group("collection_name") == "test-collection-name"

def _valid_delete_collection(match_object):
    return match_object.group("username") == "test-user-name" \
    and match_object.group("collection_name") == "test-collection-name"

def _valid_space_usage(match_object):
    return match_object.group("username") == "test-user-name" \
    and match_object.group("collection_name") == "test-collection-name"

def _valid_archive_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_list_keys(match_object):
    return match_object.group("collection_name") == "test-collection-name"

def _valid_retrieve_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_delete_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

def _valid_stat_key(match_object):
    return match_object.group("collection_name") == "test-collection-name" \
    and match_object.group("key") == "test-key"

_match_object_dispatch_table = {
    action_list_collections     : _valid_list_collections,
    action_create_collection    : _valid_create_collection,
    action_delete_collection    : _valid_delete_collection,
    action_space_usage          : _valid_space_usage,
    action_archive_key          : _valid_archive_key,
    action_list_keys            : _valid_list_keys,
    action_retrieve_key         : _valid_retrieve_key,
    action_delete_key           : _valid_delete_key,
    action_stat_key             : _valid_stat_key,
}

class TestUrlDiscriminator(unittest.TestCase):
    """test the customer authentication process"""

    def setUp(self):
        self.tearDown()

    def tearDown(self):
        pass

    def test_invalid_url(self):
        """try a URL we cant parse"""
        result = parse_url("GET", "pork")
        self.assertEqual(result, None)

    def test_valid_urls(self):
        """run through all the URLs we should be able to parse"""
        for method, url, expected_action in _valid_urls_with_actions:
            result = parse_url(method, url)
            self.assertNotEqual(result, None)
            action, match_object = result
            self.assertEqual(
                action, expected_action, 
                (action, expected_action, method, url, )
            )
            self.assertTrue(
                _match_object_dispatch_table[action](match_object),
                (method, url,)
            )

if __name__ == "__main__":
    initialize_logging(_log_path)
    unittest.main()


